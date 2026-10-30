/*
 * restart/client.c - fault-tolerant client WITHOUT session management.
 *
 * Usage:  ./client <server_ip> <server_port> <topic> <output_dir> [options]
 *         Files are written to <output_dir>/<topic>/
 *
 * HOW THIS WORKS
 * --------------
 * The server may drop the connection at any chunk boundary.  Because Case 1
 * keeps no state on either side, the only correct response is to throw away
 * what was received and start the entire topic again:
 *
 *     attempt = 0
 *     repeat:
 *         empty out <output_dir>/<topic>/        <- nothing is retained
 *         connect, send REQ_TOPIC, receive until RESP_DONE
 *         if the connection dropped: back off and try again
 *     until complete, or the retry cap is reached
 *
 * Emptying the directory is what makes this an honest Case 1 rather than an
 * accidental resume: the client must not benefit from bytes that arrived in a
 * previous attempt.  Every one of those bytes is then counted as redundant,
 * which is exactly the cost Case 2 is designed to remove.
 *
 * Retries are automatic - no manual step - and bounded: the backoff doubles
 * from 10 ms up to a 500 ms cap, and the attempt count is capped by --retries.
 * Exit status is 0 only on a complete, CRC-verified transfer.
 *
 * A dropped connection is retried.  A permanent error (unknown topic, a
 * protocol violation, a local write failure, a CRC mismatch) is not: retrying
 * those would loop forever without making progress.
 */
#include "../common/cliargs.h"
#include "../common/common.h"
#include "../common/crc32.h"
#include "../common/logging.h"
#include "../common/netutil.h"
#include "../common/protocol.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static struct {
    int         rcvbuf;
    int         nodelay;
    unsigned    max_retries;
    unsigned    backoff_ms;
    const char *log_path;
    const char *csv_path;
    const char *label;
    int         quiet;
} g_cfg = {
    .rcvbuf      = 0,
    .nodelay     = 1,
    .max_retries = DEFAULT_MAX_RETRIES,
    .backoff_ms  = DEFAULT_BACKOFF_MS_BASE,
    .log_path    = NULL,
    .csv_path    = NULL,
    .label       = "",
    .quiet       = 0,
};

/* How one attempt ended. */
typedef enum {
    ATT_OK = 0,          /* RESP_DONE received, every file verified */
    ATT_DISCONNECT = 1,  /* connection dropped: retry is correct    */
    ATT_FATAL = -1       /* permanent: retrying cannot help         */
} attempt_rc;

typedef struct {
    uint64_t wire_rx;
    uint64_t wire_tx;
    uint64_t payload_rx;
    uint64_t useful_bytes;
    uint32_t files_done;
    uint32_t crc_ok;
    uint32_t crc_fail;
} stats_t;

typedef struct {
    int      fd;
    char     name[MAX_NAME_LEN];
    uint64_t size;
    uint64_t written;
    uint32_t crc_expect;
    uint32_t crc_run;
} curfile_t;

/* Delete every regular file in `dir`.  Case 1 retains nothing between
 * attempts, so each attempt starts from an empty directory. */
static int clear_dir(const char *dir)
{
    DIR *d = opendir(dir);
    if (!d)
        return (errno == ENOENT) ? 0 : -1;

    struct dirent *e;
    int rc = 0;
    char path[MAX_PATH_LEN];
    while ((e = readdir(d)) != NULL) {
        if (strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0)
            continue;
        if ((size_t)snprintf(path, sizeof path, "%s/%s", dir, e->d_name) >= sizeof path)
            continue;
        struct stat st;
        if (stat(path, &st) == 0 && S_ISREG(st.st_mode) && unlink(path) < 0) {
            log_warn("cannot remove %s: %s", path, strerror(errno));
            rc = -1;
        }
    }
    closedir(d);
    return rc;
}

static void curfile_reset(curfile_t *f)
{
    if (f->fd >= 0)
        close(f->fd);
    f->fd      = -1;
    f->written = 0;
    f->crc_run = CRC32_INIT;
    f->name[0] = '\0';
}

/* Close the current file and verify its CRC.  Returns 0 if intact, -1 if not. */
static int curfile_finish(curfile_t *f, stats_t *st)
{
    if (f->fd < 0)
        return 0;
    uint32_t got = crc32_final(f->crc_run);
    int ok = (got == f->crc_expect);
    if (ok) {
        st->crc_ok++;
    } else {
        st->crc_fail++;
        log_error("CRC mismatch on %s: computed %08X, server said %08X",
                  f->name, got, f->crc_expect);
    }
    st->files_done++;
    st->useful_bytes += f->written;
    curfile_reset(f);
    return ok ? 0 : -1;
}

static int write_all(int fd, const void *buf, size_t n)
{
    const char *p = buf;
    size_t done = 0;
    while (done < n) {
        ssize_t k = write(fd, p + done, n - done);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        done += (size_t)k;
    }
    return 0;
}

/* One complete attempt on an already-connected socket. */
static attempt_rc one_attempt(int fd, const char *topic, const char *outdir,
                              stats_t *st)
{
    if (send_req_topic(fd, topic, &st->wire_tx) != IO_OK) {
        log_warn("sending REQ_TOPIC: %s", strerror(errno));
        return ATT_DISCONNECT;
    }

    uint32_t expect_files = 0;
    int      have_meta    = 0;
    curfile_t cur = { .fd = -1, .crc_run = CRC32_INIT };
    uint32_t cap = 64 * 1024;
    unsigned char *pay = malloc(cap);
    if (!pay) {
        log_error("out of memory allocating the receive buffer");
        return ATT_FATAL;
    }

    attempt_rc rc = ATT_FATAL;
    for (;;) {
        msg_hdr_t h;
        io_rc r = msg_recv_hdr(fd, &h, &st->wire_rx);
        if (r == IO_EOF || (r == IO_ERR && (errno == ECONNRESET || errno == EPIPE
                                            || errno == ETIMEDOUT))) {
            /* This is what an injected fault looks like from here. */
            log_info("connection dropped after %u/%u files (%llu payload bytes "
                     "this attempt)", st->files_done, expect_files,
                     (unsigned long long)st->payload_rx);
            rc = ATT_DISCONNECT;
            break;
        }
        if (r != IO_OK) {
            log_warn("reading a message header: %s", strerror(errno));
            rc = ATT_DISCONNECT;
            break;
        }
        if (h.payload_len > cap) {
            unsigned char *np = realloc(pay, h.payload_len);
            if (!np) {
                log_error("out of memory growing the receive buffer");
                break;
            }
            pay = np;
            cap = h.payload_len;
        }
        if (msg_recv_payload(fd, pay, h.payload_len, &st->wire_rx) != IO_OK) {
            log_info("connection dropped mid-%s", msg_type_name(h.type));
            rc = ATT_DISCONNECT;
            break;
        }

        if (h.type == MSG_RESP_ERROR) {
            uint16_t code = 0;
            char text[256] = "";
            if (parse_resp_error(pay, h.payload_len, &code, text, sizeof text) == 0)
                log_error("server error %u (%s): %s", code, err_code_name(code), text);
            else
                log_error("server sent a malformed RESP_ERROR");
            /* A server-reported error is a decision, not a glitch: do not retry. */
            rc = ATT_FATAL;
            break;
        }

        if (h.type == MSG_RESP_META) {
            if (parse_u32_only(pay, h.payload_len, &expect_files) < 0) {
                log_error("malformed RESP_META");
                break;
            }
            have_meta = 1;
            continue;
        }
        if (!have_meta) {
            log_error("server sent %s before RESP_META", msg_type_name(h.type));
            break;
        }

        if (h.type == MSG_RESP_FILE_INFO) {
            if (curfile_finish(&cur, st) < 0) { rc = ATT_FATAL; break; }
            if (parse_resp_file_info(pay, h.payload_len, cur.name, sizeof cur.name,
                                     &cur.size, &cur.crc_expect) < 0) {
                log_error("malformed RESP_FILE_INFO");
                break;
            }
            if (name_safe_file(cur.name) < 0) {
                log_error("server offered an unsafe file name; refusing");
                break;
            }
            char path[MAX_PATH_LEN];
            if ((size_t)snprintf(path, sizeof path, "%s/%s", outdir, cur.name)
                >= sizeof path) {
                log_error("output path for %s is too long", cur.name);
                break;
            }
            cur.fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
            if (cur.fd < 0) {
                log_errno(LOG_ERROR, path);
                break;      /* a local filesystem problem is not retryable */
            }
            cur.written = 0;
            cur.crc_run = CRC32_INIT;
            continue;
        }

        if (h.type == MSG_RESP_FILE_CHUNK) {
            if (cur.fd < 0) {
                log_error("RESP_FILE_CHUNK arrived with no file open");
                break;
            }
            if (cur.written + h.payload_len > cur.size) {
                log_error("%s: chunk overruns the announced size", cur.name);
                break;
            }
            if (write_all(cur.fd, pay, h.payload_len) < 0) {
                log_errno(LOG_ERROR, "writing to the output file");
                break;
            }
            cur.crc_run     = crc32_update(cur.crc_run, pay, h.payload_len);
            cur.written    += h.payload_len;
            st->payload_rx += h.payload_len;
            continue;
        }

        if (h.type == MSG_RESP_DONE) {
            if (curfile_finish(&cur, st) < 0) { rc = ATT_FATAL; break; }
            if (st->files_done != expect_files) {
                log_error("RESP_DONE after %u files, but %u were announced",
                          st->files_done, expect_files);
                break;
            }
            rc = ATT_OK;
            break;
        }

        log_error("unexpected message type 0x%02x (%s)", h.type, msg_type_name(h.type));
        break;
    }

    curfile_reset(&cur);
    free(pay);
    return rc;
}

static void usage(const char *argv0, const opt_spec_t *specs, size_t n)
{
    fprintf(stderr,
        "usage: %s <server_ip> <server_port> <topic> <output_dir> [options]\n"
        "\n"
        "  files are written to <output_dir>/<topic>/\n"
        "  the transfer is retried automatically until it completes\n"
        "\n"
        "options (all optional, every one has a working default):\n", argv0);
    opts_usage(specs, n, stderr);
}

int main(int argc, char **argv)
{
    const opt_spec_t specs[] = {
        { "rcvbuf",  OPT_INT,  &g_cfg.rcvbuf,      "SO_RCVBUF in bytes (default: kernel's)" },
        { "nodelay", OPT_INT,  &g_cfg.nodelay,     "1 to disable Nagle, 0 to leave it on (default 1)" },
        { "retries", OPT_UINT, &g_cfg.max_retries, "maximum reconnection attempts (default 500)" },
        { "backoff", OPT_UINT, &g_cfg.backoff_ms,  "initial backoff in ms, doubling to 500 (default 10)" },
        { "log",     OPT_STR,  &g_cfg.log_path,    "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path,    "append a result row to this CSV" },
        { "label",   OPT_STR,  &g_cfg.label,       "tag copied into the CSV row" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,       "log warnings and errors only" },
    };
    const size_t nspecs = sizeof specs / sizeof specs[0];

    if (argc < 5) {
        usage(argv[0], specs, nspecs);
        return 2;
    }
    const char *ip      = argv[1];
    const char *topic   = argv[3];
    const char *outroot = argv[4];
    uint16_t    port;
    if (parse_port(argv[2], &port) < 0) {
        fprintf(stderr, "error: '%s' is not a valid TCP port (1-65535)\n", argv[2]);
        return 2;
    }
    if (opts_parse(argc, argv, 5, specs, nspecs) < 0)
        return 2;

    log_init(g_cfg.log_path, g_cfg.quiet ? LOG_WARN : LOG_INFO);
    signal(SIGPIPE, SIG_IGN);

    if (name_safe_topic(topic) < 0) {
        log_error("'%s' is not a valid topic name", topic);
        return 2;
    }
    char outdir[MAX_PATH_LEN];
    if ((size_t)snprintf(outdir, sizeof outdir, "%s/%s", outroot, topic) >= sizeof outdir) {
        log_error("output directory path is too long");
        return 2;
    }
    if (mkdir_p(outdir, 0755) < 0) {
        log_errno(LOG_ERROR, outdir);
        return 1;
    }

    /* ---- the retry loop: this is all Case 1 can do about a fault ---- */
    stats_t agg;              /* accumulated over every attempt        */
    stats_t last;             /* the attempt that finally succeeded    */
    memset(&agg, 0, sizeof agg);
    memset(&last, 0, sizeof last);

    double   t0        = mono_now();
    unsigned attempts  = 0;   /* total attempts, including the first   */
    unsigned reconnects = 0;  /* attempts caused by a dropped connection */
    attempt_rc rc      = ATT_FATAL;

    for (;;) {
        attempts++;

        /* Case 1 keeps no state: discard anything a previous attempt left. */
        if (attempts > 1 && clear_dir(outdir) < 0)
            log_warn("could not fully clear %s before retrying", outdir);

        int fd = net_connect(ip, port);
        if (fd < 0) {
            log_warn("attempt %u: cannot connect to %s:%u: %s", attempts, ip,
                     port, strerror(errno));
            rc = ATT_DISCONNECT;
        } else {
            if (g_cfg.nodelay)
                net_set_nodelay(fd, 1);
            if (g_cfg.rcvbuf > 0)
                net_set_rcvbuf(fd, g_cfg.rcvbuf);

            stats_t att;
            memset(&att, 0, sizeof att);
            rc = one_attempt(fd, topic, outdir, &att);
            close(fd);

            /* Wire and payload counters accumulate across attempts; the useful
             * byte count is whatever the successful attempt delivered. */
            agg.wire_rx    += att.wire_rx;
            agg.wire_tx    += att.wire_tx;
            agg.payload_rx += att.payload_rx;
            if (rc == ATT_OK)
                last = att;
        }

        if (rc == ATT_OK)
            break;
        if (rc == ATT_FATAL) {
            log_error("attempt %u failed permanently; not retrying", attempts);
            break;
        }

        reconnects++;
        if (reconnects > g_cfg.max_retries) {
            log_error("giving up after %u reconnection attempts (--retries %u)",
                      reconnects, g_cfg.max_retries);
            break;
        }
        /* Exponential backoff with a cap: shift by at most 6 so the doubling
         * cannot overflow, then clamp to the configured ceiling. */
        unsigned shift = reconnects < 6 ? reconnects : 6;
        unsigned wait  = g_cfg.backoff_ms << shift;
        if (wait > DEFAULT_BACKOFF_MS_MAX)
            wait = DEFAULT_BACKOFF_MS_MAX;
        log_info("retrying from the beginning in %u ms (reconnect %u)", wait, reconnects);
        sleep_ms(wait);
    }

    double elapsed = mono_now() - t0;
    int success = (rc == ATT_OK && last.crc_fail == 0);

    /* Redundant bytes: everything received that did not survive into the final
     * output.  In Case 1 that is every byte of every abandoned attempt. */
    uint64_t redundant = agg.payload_rx > last.useful_bytes
                       ? agg.payload_rx - last.useful_bytes : 0;
    double goodput = elapsed > 0.0 ? (double)last.useful_bytes * 8.0 / elapsed / 1e6 : 0.0;
    double overhead = last.useful_bytes
                    ? (double)(agg.wire_rx - last.useful_bytes) * 100.0
                      / (double)last.useful_bytes : 0.0;

    printf("RESULT case=restart topic=%s status=%s files=%u useful_bytes=%llu "
           "payload_bytes=%llu redundant_bytes=%llu wire_rx_bytes=%llu "
           "wire_tx_bytes=%llu elapsed_s=%.6f goodput_mbps=%.3f overhead_pct=%.4f "
           "reconnects=%u retries=%u attempts=%u faults=%u crc_ok=%u crc_fail=%u\n",
           topic, success ? "ok" : "fail", last.files_done,
           (unsigned long long)last.useful_bytes,
           (unsigned long long)agg.payload_rx,
           (unsigned long long)redundant,
           (unsigned long long)agg.wire_rx,
           (unsigned long long)agg.wire_tx,
           elapsed, goodput, overhead, reconnects, reconnects, attempts,
           reconnects, last.crc_ok, last.crc_fail);
    fflush(stdout);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "label,case,topic,status,files,useful_bytes,payload_bytes,"
                   "redundant_bytes,wire_rx_bytes,wire_tx_bytes,elapsed_s,"
                   "goodput_mbps,overhead_pct,reconnects,retries,faults,"
                   "crc_ok,crc_fail",
                   "%s,restart,%s,%s,%u,%llu,%llu,%llu,%llu,%llu,%.6f,%.3f,"
                   "%.4f,%u,%u,%u,%u,%u",
                   g_cfg.label, topic, success ? "ok" : "fail", last.files_done,
                   (unsigned long long)last.useful_bytes,
                   (unsigned long long)agg.payload_rx,
                   (unsigned long long)redundant,
                   (unsigned long long)agg.wire_rx,
                   (unsigned long long)agg.wire_tx,
                   elapsed, goodput, overhead, reconnects, reconnects,
                   reconnects, last.crc_ok, last.crc_fail);

    if (success)
        log_info("transfer of '%s' complete: %u files, %llu useful bytes in "
                 "%u attempt(s), %llu redundant bytes re-received, %.3f s, "
                 "%.1f Mbit/s", topic, last.files_done,
                 (unsigned long long)last.useful_bytes, attempts,
                 (unsigned long long)redundant, elapsed, goodput);

    log_close();
    return success ? 0 : 1;
}
