/*
 * test_common.c - unit tests for the common/ library.
 *
 * HOW THIS WORKS
 * --------------
 * A tiny assertion harness (CHECK) counts failures instead of aborting, so one
 * run reports every problem rather than only the first.  The socket tests use
 * socketpair(), which gives a real AF_UNIX stream socket pair in-process: the
 * framing code is exercised through actual send()/recv() calls, not a mock.
 *
 * Build and run with:  make -C tests test
 */
#include "../src/common/common.h"
#include "../src/common/cliargs.h"
#include "../src/common/crc32.h"
#include "../src/common/fault_inject.h"
#include "../src/common/logging.h"
#include "../src/common/protocol.h"
#include "../src/common/topic_table.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

static int g_fail;
static int g_total;

#define CHECK(cond, ...)                                                      \
    do {                                                                      \
        g_total++;                                                            \
        if (!(cond)) {                                                        \
            g_fail++;                                                         \
            printf("  FAIL %s:%d  ", __FILE__, __LINE__);                     \
            printf(__VA_ARGS__);                                              \
            printf("\n");                                                     \
        }                                                                     \
    } while (0)

/* ------------------------------- crc32 --------------------------------- */

/* An independent, deliberately naive CRC-32: one byte at a time through a
 * table built on the spot.  crc32.c uses a much faster slicing-by-8 loop, and
 * this reference is what proves the two agree bit for bit. */
static uint32_t ref_crc32(const unsigned char *p, size_t n)
{
    uint32_t tab[256];
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1u) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        tab[i] = c;
    }
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < n; i++)
        crc = tab[(crc ^ p[i]) & 0xFFu] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

static void test_crc32(void)
{
    puts("test_crc32");
    /* The canonical CRC-32 check value: "123456789" -> 0xCBF43926. */
    CHECK(crc32_buf("123456789", 9) == 0xCBF43926u,
          "crc32(\"123456789\") = 0x%08X, want 0xCBF43926",
          crc32_buf("123456789", 9));
    CHECK(crc32_buf("", 0) == 0u, "crc32 of empty input should be 0");

    /* Incremental folding must match a single-shot computation. */
    uint32_t r = CRC32_INIT;
    r = crc32_update(r, "12345", 5);
    r = crc32_update(r, "6789", 4);
    CHECK(crc32_final(r) == 0xCBF43926u, "incremental crc32 mismatch");

    /* crc32.c processes 8 bytes per iteration (slicing by 8) with a
     * byte-at-a-time tail.  Cross-check it against an independent
     * byte-at-a-time reference for every length from 0 to 300, which covers
     * each possible tail length and many whole-word iterations, and for a
     * handful of split points so the carry of the running value between calls
     * is exercised too. */
    unsigned char data[300];
    for (size_t i = 0; i < sizeof data; i++)
        data[i] = (unsigned char)(i * 37u + 11u);

    int len_mismatch = 0, split_mismatch = 0;
    for (size_t n = 0; n <= sizeof data; n++) {
        if (crc32_buf(data, n) != ref_crc32(data, n))
            len_mismatch++;
        for (size_t cut = 0; cut <= n; cut += 3) {
            uint32_t acc = CRC32_INIT;
            acc = crc32_update(acc, data, cut);
            acc = crc32_update(acc, data + cut, n - cut);
            if (crc32_final(acc) != ref_crc32(data, n))
                split_mismatch++;
        }
    }
    CHECK(len_mismatch == 0, "%d lengths disagreed with the reference", len_mismatch);
    CHECK(split_mismatch == 0, "%d split points disagreed with the reference",
          split_mismatch);
}

/* --------------------------- pack and parse ---------------------------- */

static void test_pack_parse(void)
{
    puts("test_pack_parse");
    uint8_t buf[32];
    pb_t w;
    pb_init(&w, buf, sizeof buf);
    pb_u8(&w, 0xAB);
    pb_u16(&w, 0x1234);
    pb_u32(&w, 0xDEADBEEFu);
    pb_u64(&w, 0x0102030405060708ull);
    CHECK(pb_ok(&w) == 0, "packing should fit");
    CHECK(w.len == 15, "packed length = %zu, want 15", w.len);

    /* Big-endian on the wire: check the raw bytes, not just the round-trip. */
    CHECK(buf[1] == 0x12 && buf[2] == 0x34, "u16 not big-endian");
    CHECK(buf[3] == 0xDE && buf[6] == 0xEF, "u32 not big-endian");
    CHECK(buf[7] == 0x01 && buf[14] == 0x08, "u64 not big-endian");

    pv_t v;
    pv_init(&v, buf, w.len);
    CHECK(pv_u8(&v)  == 0xAB,                 "u8 round-trip");
    CHECK(pv_u16(&v) == 0x1234,               "u16 round-trip");
    CHECK(pv_u32(&v) == 0xDEADBEEFu,          "u32 round-trip");
    CHECK(pv_u64(&v) == 0x0102030405060708ull,"u64 round-trip");
    CHECK(pv_done(&v) == 0, "reader should be clean and fully consumed");

    /* Overflow: a put that does not fit must set the flag, not scribble. */
    uint8_t small[3];
    pb_t w2;
    pb_init(&w2, small, sizeof small);
    pb_u32(&w2, 1);
    CHECK(pb_ok(&w2) == -1, "overflow must be reported");

    /* Underflow: reading past the end must be reported by pv_done(). */
    pv_t v2;
    pv_init(&v2, buf, 2);
    (void)pv_u32(&v2);
    CHECK(pv_done(&v2) == -1, "underflow must be reported");

    /* Trailing bytes left unconsumed are treated as malformed. */
    pv_t v3;
    pv_init(&v3, buf, 8);
    (void)pv_u32(&v3);
    CHECK(pv_done(&v3) == -1, "unconsumed trailing bytes must be reported");
}

/* ----------------------------- name safety ----------------------------- */

static void test_name_safety(void)
{
    puts("test_name_safety");
    CHECK(name_safe_topic("gallina")   == 0, "plain topic name must pass");
    CHECK(name_safe_file("OIP-x.jpeg") == 0, "plain file name must pass");

    CHECK(name_safe_topic("")        == -1, "empty name must be rejected");
    CHECK(name_safe_topic("..")      == -1, "'..' must be rejected");
    CHECK(name_safe_topic(".")       == -1, "'.' must be rejected");
    CHECK(name_safe_topic("/etc")    == -1, "leading slash must be rejected");
    CHECK(name_safe_topic("a/b")     == -1, "embedded slash must be rejected");
    CHECK(name_safe_topic("../../x") == -1, "traversal must be rejected");
    CHECK(name_safe_file("a..b")     == -1, "embedded '..' must be rejected");

    char toolong[MAX_KEY_LEN + 8];
    memset(toolong, 'x', sizeof toolong - 1);
    toolong[sizeof toolong - 1] = '\0';
    CHECK(name_safe_topic(toolong) == -1, "over-long name must be rejected");
}

/* ------------------------- framing over a socket ----------------------- */

static void test_framing(void)
{
    puts("test_framing");
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        printf("  SKIP socketpair: %s\n", strerror(errno));
        return;
    }

    uint64_t wire_tx = 0, wire_rx = 0;

    /* 1. A typed message round-trips and the wire counters agree. */
    CHECK(send_req_topic(sv[0], "gallina", &wire_tx) == IO_OK, "send REQ_TOPIC");
    msg_hdr_t h;
    CHECK(msg_recv_hdr(sv[1], &h, &wire_rx) == IO_OK, "recv header");
    CHECK(h.type == MSG_REQ_TOPIC, "type = 0x%02x, want REQ_TOPIC", h.type);
    CHECK(h.payload_len == 2 + 7, "payload_len = %u, want 9", h.payload_len);

    uint8_t pay[64];
    CHECK(msg_recv_payload(sv[1], pay, h.payload_len, &wire_rx) == IO_OK,
          "recv payload");
    char topic[MAX_KEY_LEN];
    CHECK(parse_topic_name(pay, h.payload_len, topic, sizeof topic) == 0,
          "parse topic");
    CHECK(strcmp(topic, "gallina") == 0, "topic = '%s'", topic);
    CHECK(wire_tx == wire_rx, "wire counters: tx=%llu rx=%llu",
          (unsigned long long)wire_tx, (unsigned long long)wire_rx);
    CHECK(wire_tx == MSG_HEADER_SIZE + 9, "wire bytes = %llu, want 15",
          (unsigned long long)wire_tx);

    /* 2. msg_send_v must put header + prefix + data on the wire contiguously,
     *    which is what the Enhanced case's RESP_RANGE_CHUNK relies on. */
    uint8_t pfx[SZ_RANGE_CHUNK_PFX];
    pb_t w;
    pb_init(&w, pfx, sizeof pfx);
    pb_u32(&w, 7);
    pb_u64(&w, 0x1000);
    const char *data = "0123456789";
    CHECK(msg_send_v(sv[0], MSG_RESP_RANGE_CHUNK, FLAG_NONE,
                     pfx, sizeof pfx, data, 10, NULL) == IO_OK, "send_v");
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_OK, "recv_v header");
    CHECK(h.payload_len == SZ_RANGE_CHUNK_PFX + 10,
          "send_v payload_len = %u, want 22", h.payload_len);
    CHECK(msg_recv_payload(sv[1], pay, h.payload_len, NULL) == IO_OK, "recv_v payload");
    uint32_t idx; uint64_t off; const uint8_t *dp; uint32_t dlen;
    CHECK(parse_range_chunk_pfx(pay, h.payload_len, &idx, &off, &dp, &dlen) == 0,
          "parse range chunk prefix");
    CHECK(idx == 7 && off == 0x1000 && dlen == 10, "range chunk fields");
    CHECK(dp && memcmp(dp, data, 10) == 0, "range chunk data");

    /* 3. A zero-length payload (RESP_DONE) must round-trip. */
    CHECK(send_resp_done(sv[0], NULL) == IO_OK, "send RESP_DONE");
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_OK, "recv RESP_DONE");
    CHECK(h.type == MSG_RESP_DONE && h.payload_len == 0, "RESP_DONE shape");

    /* 4. RESP_ERROR carries code and text. */
    CHECK(send_resp_error(sv[0], ERR_UNKNOWN_TOPIC, "unknown topic: %s", "zebra")
          == IO_OK, "send RESP_ERROR");
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_OK, "recv RESP_ERROR header");
    CHECK(msg_recv_payload(sv[1], pay, h.payload_len, NULL) == IO_OK, "recv err payload");
    uint16_t code; char text[128];
    CHECK(parse_resp_error(pay, h.payload_len, &code, text, sizeof text) == 0,
          "parse RESP_ERROR");
    CHECK(code == ERR_UNKNOWN_TOPIC, "error code = %u", code);
    CHECK(strcmp(text, "unknown topic: zebra") == 0, "error text = '%s'", text);

    /* 5. An over-large declared payload_len must be refused before any
     *    allocation, with errno = EPROTO. */
    uint8_t bad[MSG_HEADER_SIZE] = { MSG_RESP_FILE_CHUNK, 0, 0xFF, 0xFF, 0xFF, 0xFF };
    CHECK(io_send_all(sv[0], bad, sizeof bad, NULL) == IO_OK, "send bad header");
    errno = 0;
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_ERR, "over-large payload_len must fail");
    CHECK(errno == EPROTO, "errno = %d, want EPROTO", errno);

    /* 6. A clean close on a message boundary is IO_EOF, not an error. */
    close(sv[0]);
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_EOF, "clean close must be IO_EOF");
    close(sv[1]);
}

/* -------------------- truncated message is an error -------------------- */

static void test_truncated_message(void)
{
    puts("test_truncated_message");
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        printf("  SKIP socketpair: %s\n", strerror(errno));
        return;
    }
    /* Announce 100 payload bytes, send 10, then close.  The receiver must
     * report an error, never a short-but-successful read: this is exactly what
     * a fault-injected disconnection looks like mid-chunk. */
    uint8_t hdr[MSG_HEADER_SIZE] = { MSG_RESP_FILE_CHUNK, 0, 0, 0, 0, 100 };
    io_send_all(sv[0], hdr, sizeof hdr, NULL);
    io_send_all(sv[0], "0123456789", 10, NULL);
    close(sv[0]);

    msg_hdr_t h;
    CHECK(msg_recv_hdr(sv[1], &h, NULL) == IO_OK, "header should arrive");
    CHECK(h.payload_len == 100, "payload_len = %u", h.payload_len);
    uint8_t buf[128];
    CHECK(msg_recv_payload(sv[1], buf, 100, NULL) == IO_ERR,
          "truncated payload must be IO_ERR");
    close(sv[1]);
}

/* --------------------------- malformed payloads ------------------------ */

static void test_malformed_payloads(void)
{
    puts("test_malformed_payloads");
    char name[MAX_NAME_LEN];
    uint64_t size; uint32_t crc, v32; uint64_t sid, off; uint32_t idx;

    /* Truncated: the length prefix claims more bytes than are present. */
    uint8_t t1[] = { 0x00, 0x09, 'g', 'a', 'l' };
    CHECK(parse_topic_name(t1, sizeof t1, name, sizeof name) == -1,
          "truncated topic name must be rejected");

    /* Zero-length name must be rejected, not accepted as "". */
    uint8_t t2[] = { 0x00, 0x00 };
    CHECK(parse_topic_name(t2, sizeof t2, name, sizeof name) == -1,
          "empty topic name must be rejected");

    /* Trailing garbage after a well-formed name. */
    uint8_t t3[] = { 0x00, 0x03, 'a', 'b', 'c', 'X' };
    CHECK(parse_topic_name(t3, sizeof t3, name, sizeof name) == -1,
          "trailing bytes must be rejected");

    /* Wrong fixed sizes must be rejected for every fixed-shape parser. */
    uint8_t z[64];
    memset(z, 0, sizeof z);
    CHECK(parse_u32_only(z, 3, &v32) == -1, "short RESP_META must be rejected");
    CHECK(parse_u32_only(z, 5, &v32) == -1, "long RESP_META must be rejected");
    CHECK(parse_u32_only(z, 4, &v32) ==  0, "4-byte RESP_META must be accepted");
    CHECK(parse_sid_idx_off(z, 19, &sid, &idx, &off) == -1, "short REQ_ACK");
    CHECK(parse_sid_idx_off(z, 20, &sid, &idx, &off) ==  0, "exact REQ_ACK");
    CHECK(parse_resp_file_info(z, 5, name, sizeof name, &size, &crc) == -1,
          "short RESP_FILE_INFO");

    /* A name longer than the caller's buffer must be rejected rather than
     * truncated, so a client can never create a file under a cut-short name. */
    uint8_t big[8 + MAX_NAME_LEN];
    pb_t w;
    pb_init(&w, big, sizeof big);
    pb_u16(&w, 40);
    for (int i = 0; i < 40; i++) pb_u8(&w, 'a');
    pb_u64(&w, 1);
    pb_u32(&w, 0);
    char tiny[8];
    CHECK(parse_resp_file_info(big, (uint32_t)w.len, tiny, sizeof tiny, &size, &crc)
          == -1, "over-long name must be rejected, not truncated");
}

/* ------------------------------- cliargs ------------------------------- */

static void test_cliargs(void)
{
    puts("test_cliargs");
    uint16_t port; double p; uint64_t u;

    CHECK(parse_port("8080", &port) == 0 && port == 8080, "parse_port 8080");
    CHECK(parse_port("65535", &port) == 0, "parse_port 65535");
    CHECK(parse_port("0", &port)      == -1, "port 0 must be rejected");
    CHECK(parse_port("65536", &port)  == -1, "port 65536 must be rejected");
    CHECK(parse_port("8080x", &port)  == -1, "trailing junk must be rejected");
    CHECK(parse_port("", &port)       == -1, "empty port must be rejected");
    CHECK(parse_port("-1", &port)     == -1, "negative port must be rejected");

    CHECK(parse_prob("0", &p) == 0 && p == 0.0, "p=0");
    CHECK(parse_prob("1", &p) == 0 && p == 1.0, "p=1");
    CHECK(parse_prob("0.0001", &p) == 0, "p=1e-4");
    CHECK(parse_prob("1e-5", &p) == 0 && p == 1e-5, "p in exponent form");
    CHECK(parse_prob("1.5", &p)  == -1, "p>1 must be rejected");
    CHECK(parse_prob("-0.1", &p) == -1, "p<0 must be rejected");
    CHECK(parse_prob("abc", &p)  == -1, "non-numeric p must be rejected");

    CHECK(parse_u64("16384", &u) == 0 && u == 16384, "parse_u64");

    /* opts_parse: a known option sets its destination; an unknown one fails. */
    int conns = 0; unsigned chunk = 0; const char *logp = NULL; int flag = 0;
    const opt_spec_t specs[] = {
        { "conns", OPT_INT,   &conns, "K" },
        { "chunk", OPT_UINT,  &chunk, "bytes" },
        { "log",   OPT_STR,   &logp,  "path" },
        { "quiet", OPT_FLAG,  &flag,  "less output" },
    };
    char *ok_argv[] = { "prog", "--conns", "8", "--chunk", "32768",
                        "--log", "/tmp/x.log", "--quiet" };
    CHECK(opts_parse(8, ok_argv, 1, specs, 4) == 0, "opts_parse should succeed");
    CHECK(conns == 8 && chunk == 32768 && flag == 1, "opts_parse values");
    CHECK(logp && strcmp(logp, "/tmp/x.log") == 0, "opts_parse string value");

    char *bad_argv[] = { "prog", "--nope", "1" };
    printf("  (one 'unknown option' message below is expected)\n");
    CHECK(opts_parse(3, bad_argv, 1, specs, 4) == -1, "unknown option must fail");

    char *missing_argv[] = { "prog", "--conns" };
    printf("  (one 'needs a value' message below is expected)\n");
    CHECK(opts_parse(2, missing_argv, 1, specs, 4) == -1, "missing value must fail");
}

/* ---------------------------- fault injection -------------------------- */

static void test_fault_inject(void)
{
    puts("test_fault_inject");
    fault_seed_init(12345);
    CHECK(fault_seed_get() == 12345, "pinned seed must be used verbatim");

    /* p = 0 must never fire: this is what makes a Part II binary at p=0 a
     * fault-free control that is directly comparable with Part I. */
    fault_rng_t f0;
    fault_init(&f0, 0.0);
    int fired = 0;
    for (int i = 0; i < 100000; i++) fired += fault_should_trigger(&f0);
    CHECK(fired == 0, "p=0 fired %d times", fired);
    CHECK(f0.checks == 0, "p=0 should not even count checks");

    /* p = 1 must always fire. */
    fault_rng_t f1;
    fault_init(&f1, 1.0);
    CHECK(fault_should_trigger(&f1) == 1, "p=1 must fire");

    /* The observed rate at p=0.01 over 200k draws should be close to 0.01.
     * The tolerance is wide enough that this cannot flake: the standard
     * deviation of the estimate is sqrt(p(1-p)/n) = 0.00022, so +/-0.002 is
     * about 9 sigma. */
    fault_rng_t f2;
    fault_init(&f2, 0.01);
    const int N = 200000;
    for (int i = 0; i < N; i++) (void)fault_should_trigger(&f2);
    double rate = (double)f2.fires / (double)N;
    CHECK(rate > 0.008 && rate < 0.012, "observed rate %.5f, want ~0.01", rate);
    CHECK(f2.checks == (uint64_t)N, "check counter = %llu, want %d",
          (unsigned long long)f2.checks, N);
    printf("  observed fault rate at p=0.01 over %d draws: %.5f\n", N, rate);

    /* Two workers must get different streams, or concurrent clients would fail
     * at identical offsets and the experiment would be biased. */
    fault_rng_t a, b;
    fault_init(&a, 0.5);
    fault_init(&b, 0.5);
    CHECK(a.state != b.state, "two workers must not share RNG state");

    /* Expected-faults helper, used in the report's tables. */
    CHECK(fault_expected(1e-4, 4350) > 0.43 && fault_expected(1e-4, 4350) < 0.44,
          "expected faults helper");
}

/* ------------------------- mkdir_p and csv_append ---------------------- */

static void test_fs_helpers(const char *tmpdir)
{
    puts("test_fs_helpers");
    char deep[MAX_PATH_LEN];
    snprintf(deep, sizeof deep, "%s/a/b/c/d", tmpdir);
    CHECK(mkdir_p(deep, 0755) == 0, "mkdir_p should create nested dirs");
    struct stat st;
    CHECK(stat(deep, &st) == 0 && S_ISDIR(st.st_mode), "nested dir should exist");
    CHECK(mkdir_p(deep, 0755) == 0, "mkdir_p on an existing path must succeed");

    char csv[MAX_PATH_LEN];
    snprintf(csv, sizeof csv, "%s/r.csv", tmpdir);
    /* Start from a clean slate: csv_append() deliberately appends, so re-running
     * the test suite in the same tmpdir would otherwise accumulate rows and the
     * "header written once" assertion below would be meaningless. */
    unlink(csv);
    CHECK(csv_append(csv, "a,b,c", "1,2,%d", 3) == 0, "csv_append first row");
    CHECK(csv_append(csv, "a,b,c", "4,5,%d", 6) == 0, "csv_append second row");

    FILE *f = fopen(csv, "r");
    CHECK(f != NULL, "csv file should exist");
    if (f) {
        char l1[64] = "", l2[64] = "", l3[64] = "", l4[64] = "";
        if (!fgets(l1, sizeof l1, f)) l1[0] = '\0';
        if (!fgets(l2, sizeof l2, f)) l2[0] = '\0';
        if (!fgets(l3, sizeof l3, f)) l3[0] = '\0';
        CHECK(strcmp(l1, "a,b,c\n") == 0, "header written once, got '%s'", l1);
        CHECK(strcmp(l2, "1,2,3\n") == 0, "row 1 = '%s'", l2);
        CHECK(strcmp(l3, "4,5,6\n") == 0, "row 2 = '%s'", l3);
        CHECK(fgets(l4, sizeof l4, f) == NULL, "no extra rows");
        fclose(f);
    }
}

/* ----------------------------- topic table ----------------------------- */

static void test_topic_table(const char *root)
{
    puts("test_topic_table");
    topic_table_t t;
    if (topic_table_build(&t, root, 1) < 0) {
        printf("  SKIP topic table: cannot build from %s\n", root);
        return;
    }
    CHECK(t.n_topics == 3, "topic count = %zu, want 3", t.n_topics);

    const topic_t *alpha = topic_table_lookup(&t, "topic_alpha");
    CHECK(alpha != NULL, "topic_alpha must be found");
    CHECK(topic_table_lookup(&t, "nope") == NULL, "unknown topic must be NULL");
    CHECK(topic_table_lookup(&t, "TOPIC_ALPHA") == NULL, "lookup is case-sensitive");

    if (alpha) {
        CHECK(alpha->n_files == 15, "topic_alpha files = %zu, want 15", alpha->n_files);
        /* Alphabetical order is the contract that makes file_idx meaningful. */
        int sorted = 1;
        for (size_t i = 1; i < alpha->n_files; i++)
            if (strcmp(alpha->files[i - 1].name, alpha->files[i].name) > 0) sorted = 0;
        CHECK(sorted, "file list must be sorted alphabetically");

        /* The empty file must be present with size 0 and the CRC of nothing. */
        int found_empty = 0;
        for (size_t i = 0; i < alpha->n_files; i++)
            if (alpha->files[i].size == 0) {
                found_empty = 1;
                CHECK(alpha->files[i].crc32 == 0, "crc32 of empty file must be 0");
            }
        CHECK(found_empty, "the zero-length file must appear in the catalogue");

        char path[MAX_PATH_LEN];
        CHECK(topic_file_path(path, sizeof path, alpha, 0) == 0, "file path ok");
        CHECK(topic_file_path(path, sizeof path, alpha, alpha->n_files) == -1,
              "out-of-range index must fail");
        /* The cached CRC must match a fresh computation over the same file. */
        uint32_t fresh = 0;
        if (topic_file_path(path, sizeof path, alpha, 5) == 0 &&
            crc32_file(path, &fresh) == 0)
            CHECK(fresh == alpha->files[5].crc32, "cached crc32 must match");
    }
    topic_table_free(&t);

    topic_table_t bad;
    printf("  (one 'no topic sub-directories' or stat error below is expected)\n");
    CHECK(topic_table_build(&bad, "/nonexistent/path/xyz", 0) == -1,
          "missing root must fail cleanly");
}

int main(int argc, char **argv)
{
    log_init(NULL, LOG_WARN);          /* keep the test output readable */
    const char *synth = (argc > 1) ? argv[1] : "../../data/synthetic";
    const char *tmp   = (argc > 2) ? argv[2] : "/tmp/it305_test";

    test_crc32();
    test_pack_parse();
    test_name_safety();
    test_framing();
    test_truncated_message();
    test_malformed_payloads();
    test_cliargs();
    test_fault_inject();
    test_fs_helpers(tmp);
    test_topic_table(synth);

    printf("\n%d/%d checks passed, %d failed\n", g_total - g_fail, g_total, g_fail);
    log_close();
    return g_fail == 0 ? 0 : 1;
}
