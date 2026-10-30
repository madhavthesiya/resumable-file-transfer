/* Fuzz harness for the protocol decode path.
 *
 * The decoders are the only code in this project that consumes bytes chosen by
 * somebody else, so they are the only place where a malformed input can turn
 * into a memory-safety bug. Three targets, selected by the first input byte:
 *
 *   0  payload readers (pv_*)   - arbitrary bytes, arbitrary read sequence
 *   1  framing (msg_recv_*)     - arbitrary bytes pushed through a socketpair
 *   2  name safety              - arbitrary strings joined onto a path
 *
 * It builds two ways:
 *
 *   gcc  ... fuzz_protocol.c        -> standalone, seeded PRNG, runs for N
 *                                      seconds; this is what CI runs, because
 *                                      it needs no clang.
 *   clang -fsanitize=fuzzer,address -DFUZZ_LIBFUZZER
 *                                   -> coverage-guided libFuzzer.
 *
 * Both share fuzz_one(), so a crash found by either is reproducible by the
 * other. Failures print the offending input as hex for replay.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE   /* the Makefile defines it too; standalone builds may not */
#endif
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "../src/common/common.h"
#include "../src/common/logging.h"
#include "../src/common/protocol.h"

/* ------------------------------------------------------------------ report */

static const uint8_t *g_input;
static size_t g_input_len;

static void dump_and_die(const char *what)
{
    fprintf(stderr, "\nFUZZ FAILURE: %s\ninput (%zu bytes): ", what, g_input_len);
    for (size_t i = 0; i < g_input_len; i++)
        fprintf(stderr, "%02x", g_input[i]);
    fprintf(stderr, "\n");
    abort();
}

#define MUST(cond, msg) do { if (!(cond)) dump_and_die(msg); } while (0)

/* ------------------------------------------------- target 0: pv_* readers */

/* Invariants that must hold for ANY byte sequence and ANY read order:
 *   - the cursor never passes the length
 *   - pv_bytes returns NULL exactly when the read would underflow
 *   - once `bad` is set it stays set (no silent recovery)
 *   - pv_done() == 0 implies the whole payload was consumed without underflow
 */
static void target_payload_reader(const uint8_t *d, size_t n)
{
    if (n < 1) return;
    uint8_t script = d[0];
    const uint8_t *body = d + 1;
    size_t body_len = n - 1;

    pv_t v;
    pv_init(&v, body, body_len);
    MUST(v.off == 0, "pv_init left a non-zero offset");
    MUST(v.bad == 0, "pv_init set the bad flag");

    /* Drive a read sequence out of the script byte: two bits per step. */
    int was_bad = 0;
    for (int step = 0; step < 16; step++) {
        size_t before = v.off;
        switch ((script >> (step % 8 * 2)) & 3) {
        case 0: (void)pv_u8(&v);  break;
        case 1: (void)pv_u16(&v); break;
        case 2: (void)pv_u32(&v); break;
        case 3: {
            size_t want = (size_t)(script & 0x0f);
            const uint8_t *p = pv_bytes(&v, want);
            if (p) {
                MUST(p >= body && p + want <= body + body_len,
                     "pv_bytes returned a pointer outside the buffer");
            } else {
                MUST(v.bad, "pv_bytes returned NULL without setting bad");
            }
            break;
        }
        }
        MUST(v.off <= v.len, "payload cursor ran past the end");
        MUST(v.off >= before, "payload cursor moved backwards");
        if (was_bad) MUST(v.bad, "bad flag was cleared after being set");
        was_bad = v.bad;
    }

    if (pv_done(&v) == 0) {
        MUST(!v.bad, "pv_done reported success despite an underflow");
        MUST(v.off == v.len, "pv_done reported success with bytes left over");
    }
}

/* ------------------------------------------------ target 1: msg framing */

/* Push arbitrary bytes at the receiver and require that it either parses them
 * or rejects them - never that it believes an absurd length. The header's
 * length field is attacker-controlled and four bytes wide, so "declare 4 GB"
 * is the obvious first attack. */
static void target_framing(const uint8_t *d, size_t n)
{
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) return;

    if (write(sv[1], d, n) < 0) { close(sv[0]); close(sv[1]); return; }
    shutdown(sv[1], SHUT_WR);

    msg_hdr_t h;
    uint64_t wire = 0;
    io_rc rc = msg_recv_hdr(sv[0], &h, &wire);

    if (rc == IO_OK) {
        MUST(h.payload_len <= MAX_PAYLOAD_LEN,
             "msg_recv_hdr accepted a payload_len above MAX_PAYLOAD_LEN");

        /* A sane length may still exceed what actually arrived; the payload
         * read must then report EOF rather than hand back uninitialised bytes. */
        static uint8_t buf[MAX_PAYLOAD_LEN];
        memset(buf, 0xA5, h.payload_len ? h.payload_len : 1);
        io_rc prc = msg_recv_payload(sv[0], buf, h.payload_len, &wire);
        MUST(prc == IO_OK || prc == IO_EOF || prc == IO_ERR,
             "msg_recv_payload returned an undefined status");

        /* Feeding the decoded payload straight back through the reader is the
         * composition the real code performs. */
        if (prc == IO_OK) {
            pv_t v;
            pv_init(&v, buf, h.payload_len);
            (void)pv_u32(&v);
            (void)pv_done(&v);
        }
    } else {
        MUST(rc == IO_EOF || rc == IO_ERR, "msg_recv_hdr returned an undefined status");
    }

    close(sv[0]);
    close(sv[1]);
}

/* -------------------------------------------- target 2: name safety ---- */

/* The security property the whole project rests on: a name that arrives over
 * the wire is joined onto a directory path, so anything that escapes that
 * directory must be refused. */
static void target_name_safety(const uint8_t *d, size_t n)
{
    char name[512];
    size_t len = n < sizeof name - 1 ? n : sizeof name - 1;
    memcpy(name, d, len);
    name[len] = '\0';

    int topic_ok = name_safe_topic(name);
    int file_ok = name_safe_file(name);

    int has_slash = strchr(name, '/') != NULL;
    int is_dot = strcmp(name, ".") == 0 || strcmp(name, "..") == 0;
    int is_empty = name[0] == '\0';

    if (has_slash || is_dot || is_empty) {
        MUST(topic_ok != 0, "name_safe_topic accepted a traversal or empty name");
        MUST(file_ok != 0, "name_safe_file accepted a traversal or empty name");
    }
    /* A backslash is not a separator on POSIX, so it is allowed through; the
     * check above deliberately does not claim otherwise. */
}

/* ------------------------------------------------------------- dispatch */

void fuzz_one(const uint8_t *data, size_t size);

void fuzz_one(const uint8_t *data, size_t size)
{
    if (size < 2) return;
    g_input = data;
    g_input_len = size;

    switch (data[0] % 3) {
    case 0: target_payload_reader(data + 1, size - 1); break;
    case 1: target_framing(data + 1, size - 1);        break;
    case 2: target_name_safety(data + 1, size - 1);    break;
    }
}

#ifdef FUZZ_LIBFUZZER

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_one(data, size);
    return 0;
}

#else /* ------------------------------------------ standalone driver ---- */

/* Hand-written inputs for the cases a random generator would take a long time
 * to stumble onto. Each is a target selector byte followed by a body. */
static const char *const SEEDS[] = {
    "\x02../",            /* parent directory                               */
    "\x02..",             /* bare dotdot                                    */
    "\x02.",              /* bare dot                                       */
    "\x02/etc/passwd",    /* absolute path                                  */
    "\x02a/../../b",      /* traversal in the middle                        */
    "\x02",               /* empty name                                     */
    "\x01\x04\x00\xff\xff\xff\xff",   /* header declaring a 4 GB payload    */
    "\x01\x04\x00\x00\x00\x00\x00",   /* header declaring a zero payload    */
    "\x01\x00\x00\xff\xff\xff\xf0",   /* unknown type, huge length          */
    "\x00\xff",                        /* reader script, empty body         */
    "\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08",
};

static uint64_t rng_state;

static uint32_t rnd(void)
{
    /* xorshift64*, so a run is reproducible from its printed seed. */
    rng_state ^= rng_state >> 12;
    rng_state ^= rng_state << 25;
    rng_state ^= rng_state >> 27;
    return (uint32_t)((rng_state * 0x2545F4914F6CDD1DULL) >> 32);
}

int main(int argc, char **argv)
{
    unsigned seconds = 10;
    if (argc > 1) seconds = (unsigned)strtoul(argv[1], NULL, 10);

    /* The decoders log every rejection, and rejection is the common case here.
     * Silence the library so a real failure is the only thing on stderr. */
    log_init(NULL, (log_level_t)(LOG_ERROR + 1));

    const char *env = getenv("FUZZ_SEED");
    rng_state = env ? strtoull(env, NULL, 10) : (uint64_t)time(NULL);
    if (rng_state == 0) rng_state = 0x9E3779B97F4A7C15ULL;
    uint64_t seed = rng_state;

    printf("protocol fuzzer: %u seconds, seed %llu\n",
           seconds, (unsigned long long)seed);
    printf("  (replay a failure with FUZZ_SEED=%llu)\n", (unsigned long long)seed);

    size_t n_seeds = sizeof SEEDS / sizeof SEEDS[0];
    for (size_t i = 0; i < n_seeds; i++)
        fuzz_one((const uint8_t *)SEEDS[i], strlen(SEEDS[i]) + 1);
    printf("  %zu seed inputs passed\n", n_seeds);

    uint8_t buf[4096];
    unsigned long iters = 0;
    time_t deadline = time(NULL) + (time_t)seconds;

    while (time(NULL) < deadline) {
        for (int batch = 0; batch < 2000; batch++) {
            size_t len = 2 + rnd() % (sizeof buf - 2);
            for (size_t i = 0; i < len; i++) buf[i] = (uint8_t)rnd();

            /* Half the inputs are steered at a well-formed header, so the
             * framing target gets past its first check often enough to matter. */
            if ((rnd() & 1) && len > 8) {
                buf[0] = 1;
                buf[1] = (uint8_t)(rnd() % 0x29);
                buf[2] = 0;
                uint32_t plen = rnd() % (MAX_PAYLOAD_LEN + 64);
                buf[3] = (uint8_t)(plen >> 24); buf[4] = (uint8_t)(plen >> 16);
                buf[5] = (uint8_t)(plen >> 8);  buf[6] = (uint8_t)plen;
            }
            fuzz_one(buf, len);
            iters++;
        }
    }

    printf("  %lu random inputs passed\n", iters);
    printf("protocol fuzzer: no failures\n");
    return 0;
}

#endif
