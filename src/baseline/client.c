/*
 * baseline/client.c - fetches every file of one topic and writes it to disk.
 *
 * Usage:  ./client <server_host> <server_port> <topic> <output_dir> [options]
 *         Files are written to <output_dir>/<topic>/
 *
 * HOW THIS WORKS
 * --------------
 * One connection, one request, then a loop that dispatches on the type of each
 * arriving message:
 *
 *   RESP_META       -> how many files to expect
 *   RESP_FILE_INFO  -> create the next output file; remember its size and CRC
 *   RESP_FILE_CHUNK -> append to the current file, folding the bytes into a
 *                      running CRC as they go past
 *   RESP_DONE       -> the topic is complete; verify the file count and stop
 *   RESP_ERROR      -> print the server's reason and exit non-zero
 *
 * The CRC is computed while the data streams by, so verification costs one pass
 * and no re-read.  A mismatch is fatal: the exit status is non-zero, because a
 * silently corrupt download is the one outcome a file distribution service must
 * never report as success.
 *
 * On completion a single machine-readable RESULT line goes to stdout (logs go
 * to stderr), which is what the benchmark harness parses.
 */
#include "../common/cliargs.h"
#include "../common/common.h"
#include "../common/crc32.h"
#include "../common/logging.h"
#include "../common/netutil.h"
#include "../common/protocol.h"

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
    const char *log_path;
    const char *csv_path;
    const char *label;     /* free-form tag copied into the CSV row */
    int         quiet;
} g_cfg = { 0, 1, NULL, NULL, "", 0 };

/* Everything the RESULT line and the CSV row are built from. */
typedef struct {
    uint64_t wire_rx;        /* all bytes read from the socket    */
    uint64_t wire_tx;        /* all bytes written to the socket   */
    uint64_t payload_rx;     /* file data bytes received          */
    uint64_t useful_bytes;   /* file data bytes kept on disk      */
    uint32_t files_done;
    uint32_t crc_ok;
    uint32_t crc_fail;
} stats_t;

/* State of the file currently being written. */
typedef struct {
    int      fd;
    char     name[MAX_NAME_LEN];
    uint64_t size;
    uint64_t written;
    uint32_t crc_expect;
    uint32_t crc_run;
} curfile_t;

static void curfile_reset(curfile_t *f)
{
    if (f->fd >= 0)
        close(f->fd);
    f->fd      = -1;
    f->written = 0;
    f->crc_run = CRC32_INIT;
    f->name[0] = '\0';
}

/* Finish the current file: close it and check its CRC.
 * Returns 0 if the file is intact, -1 on a mismatch. */
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

/* Write exactly `n` bytes, looping over short writes and retrying EINTR. */
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

/* Run the whole transfer on an already-connected socket.
 * Returns 0 on success, -1 on failure (the reason is logged). */
static int receive_topic(int fd, const char *topic, const char *outdir,
                         stats_t *st)
{
    if (send_req_topic(fd, topic, &st->wire_tx) != IO_OK) {
        log_errno(LOG_ERROR, "sending REQ_TOPIC");
        return -1;
    }

    uint32_t expect_files = 0;
    int      have_meta    = 0;
    curfile_t cur = { .fd = -1, .crc_run = CRC32_INIT };
    /* One payload buffer, reused for every message.  It starts at a size that
     * covers the default 16 KiB chunk comfortably and grows if the server was
     * started with a larger --chunk, which the client cannot know in advance.
     * Starting small matters: 8 concurrent clients each pre-allocating the
     * 16 MiB protocol maximum would reserve 128 MiB for nothing. */
    uint32_t cap = 64 * 1024;
    unsigned char *pay = malloc(cap);
    if (!pay) {
        log_error("out of memory allocating the receive buffer");
        return -1;
    }

    int rc = -1;
    for (;;) {
        msg_hdr_t h;
        io_rc r = msg_recv_hdr(fd, &h, &st->wire_rx);
        if (r == IO_EOF) {
            log_error("server closed the connection before RESP_DONE "
                      "(after %u of %u files)", st->files_done, expect_files);
            break;
        }
        if (r != IO_OK) {
            log_errno(LOG_ERROR, "reading a message header");
            break;
        }
        if (h.payload_len > cap) {
            /* msg_recv_hdr() has already refused anything above
             * MAX_PAYLOAD_LEN, so this growth is bounded. */
            unsigned char *np = realloc(pay, h.payload_len);
            if (!np) {
                log_error("out of memory growing the receive buffer to %u bytes",
                          h.payload_len);
                break;
            }
            pay = np;
            cap = h.payload_len;
        }
        if (msg_recv_payload(fd, pay, h.payload_len, &st->wire_rx) != IO_OK) {
            log_error("truncated %s payload", msg_type_name(h.type));
            break;
        }

        if (h.type == MSG_RESP_META) {
            if (parse_u32_only(pay, h.payload_len, &expect_files) < 0) {
                log_error("malformed RESP_META");
                break;
            }
            have_meta = 1;
            log_info("topic '%s': %u files announced", topic, expect_files);
            continue;
        }

        if (h.type == MSG_RESP_ERROR) {
            uint16_t code = 0;
            char text[256] = "";
            if (parse_resp_error(pay, h.payload_len, &code, text, sizeof text) == 0)
                log_error("server error %u (%s): %s", code, err_code_name(code), text);
            else
                log_error("server sent a malformed RESP_ERROR");
            break;
        }

        if (!have_meta) {
            log_error("server sent %s before RESP_META", msg_type_name(h.type));
            break;
        }

        if (h.type == MSG_RESP_FILE_INFO) {
            /* Finish the previous file before starting the next one. */
            if (curfile_finish(&cur, st) < 0)
                break;

            if (parse_resp_file_info(pay, h.payload_len, cur.name, sizeof cur.name,
                                     &cur.size, &cur.crc_expect) < 0) {
                log_error("malformed RESP_FILE_INFO");
                break;
            }
            /* The server chose this name, but it still gets validated: the
             * client must not create a path outside its own output directory
             * just because the other end asked it to. */
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
                break;
            }
            cur.written = 0;
            cur.crc_run = CRC32_INIT;
            log_debug("receiving %s (%llu bytes)", cur.name,
                      (unsigned long long)cur.size);
            continue;
        }

        if (h.type == MSG_RESP_FILE_CHUNK) {
            if (cur.fd < 0) {
                log_error("RESP_FILE_CHUNK arrived with no file open");
                break;
            }
            if (cur.written + h.payload_len > cur.size) {
                /* More data than the announced size: refuse it rather than
                 * writing past what RESP_FILE_INFO promised. */
                log_error("%s: chunk overruns the announced size "
                          "(%llu + %u > %llu)", cur.name,
                          (unsigned long long)cur.written, h.payload_len,
                          (unsigned long long)cur.size);
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
            if (curfile_finish(&cur, st) < 0)
                break;
            if (st->files_done != expect_files) {
                log_error("RESP_DONE after %u files, but %u were announced",
                          st->files_done, expect_files);
                break;
            }
            rc = 0;
            break;
        }

        log_error("unexpected message type 0x%02x (%s)", h.type,
                  msg_type_name(h.type));
        break;
    }

    curfile_reset(&cur);
    free(pay);
    return rc;
}

static void usage(const char *argv0, const opt_spec_t *specs, size_t n)
{
    fprintf(stderr,
        "usage: %s <server_host> <server_port> <topic> <output_dir> [options]\n"
        "\n"
        "  files are written to <output_dir>/<topic>/\n"
        "\n"
        "options (all optional, every one has a working default):\n", argv0);
    opts_usage(specs, n, stderr);
}

int main(int argc, char **argv)
{
    const opt_spec_t specs[] = {
        { "rcvbuf",  OPT_INT,  &g_cfg.rcvbuf,   "SO_RCVBUF in bytes (default: kernel's)" },
        { "nodelay", OPT_INT,  &g_cfg.nodelay,  "1 to disable Nagle, 0 to leave it on (default 1)" },
        { "log",     OPT_STR,  &g_cfg.log_path, "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path, "append a result row to this CSV" },
        { "label",   OPT_STR,  &g_cfg.label,    "tag copied into the CSV row" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,    "log warnings and errors only" },
    };
    const size_t nspecs = sizeof specs / sizeof specs[0];

    if (argc < 5) {
        usage(argv[0], specs, nspecs);
        return 2;
    }
    const char *ip    = argv[1];
    uint16_t    port;
    const char *topic = argv[3];
    const char *outroot = argv[4];

    if (parse_port(argv[2], &port) < 0) {
        fprintf(stderr, "error: '%s' is not a valid TCP port (1-65535)\n", argv[2]);
        return 2;
    }
    if (opts_parse(argc, argv, 5, specs, nspecs) < 0)
        return 2;

    log_init(g_cfg.log_path, g_cfg.quiet ? LOG_WARN : LOG_INFO);
    /* A server that vanishes mid-transfer must surface as EPIPE on send(), not
     * as a signal that kills the client. */
    signal(SIGPIPE, SIG_IGN);

    if (name_safe_topic(topic) < 0) {
        log_error("'%s' is not a valid topic name", topic);
        return 2;
    }

    /* The topic sub-directory is created up front, exactly as the assignment
     * requires: output lands in <output_dir>/<topic>/. */
    char outdir[MAX_PATH_LEN];
    if ((size_t)snprintf(outdir, sizeof outdir, "%s/%s", outroot, topic)
        >= sizeof outdir) {
        log_error("output directory path is too long");
        return 2;
    }
    if (mkdir_p(outdir, 0755) < 0) {
        log_errno(LOG_ERROR, outdir);
        return 1;
    }

    stats_t st;
    memset(&st, 0, sizeof st);
    double t0 = mono_now();

    int fd = net_connect(ip, port);
    if (fd < 0) {
        log_error("cannot connect to %s:%u: %s", ip, port, strerror(errno));
        return 1;
    }
    if (g_cfg.nodelay)
        net_set_nodelay(fd, 1);
    if (g_cfg.rcvbuf > 0)
        net_set_rcvbuf(fd, g_cfg.rcvbuf);

    int rc = receive_topic(fd, topic, outdir, &st);
    close(fd);

    double elapsed = mono_now() - t0;
    double goodput = elapsed > 0.0 ? (double)st.useful_bytes * 8.0 / elapsed / 1e6 : 0.0;
    /* Part I has no retransmission, so redundant bytes should always be 0 here.
     * The field exists so that one RESULT format covers Part I and Part II. */
    uint64_t redundant = st.payload_rx > st.useful_bytes
                       ? st.payload_rx - st.useful_bytes : 0;
    double overhead = st.useful_bytes
                    ? (double)(st.wire_rx - st.useful_bytes) * 100.0
                      / (double)st.useful_bytes : 0.0;
    const char *status = (rc == 0 && st.crc_fail == 0) ? "ok" : "fail";

    /* One parseable line on stdout; everything else went to stderr. */
    printf("RESULT case=baseline topic=%s status=%s files=%u useful_bytes=%llu "
           "payload_bytes=%llu redundant_bytes=%llu wire_rx_bytes=%llu "
           "wire_tx_bytes=%llu elapsed_s=%.6f goodput_mbps=%.3f overhead_pct=%.4f "
           "reconnects=0 retries=0 faults=0 crc_ok=%u crc_fail=%u\n",
           topic, status, st.files_done,
           (unsigned long long)st.useful_bytes,
           (unsigned long long)st.payload_rx,
           (unsigned long long)redundant,
           (unsigned long long)st.wire_rx,
           (unsigned long long)st.wire_tx,
           elapsed, goodput, overhead, st.crc_ok, st.crc_fail);
    fflush(stdout);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "label,case,topic,status,files,useful_bytes,payload_bytes,"
                   "redundant_bytes,wire_rx_bytes,wire_tx_bytes,elapsed_s,"
                   "goodput_mbps,overhead_pct,reconnects,retries,faults,"
                   "crc_ok,crc_fail",
                   "%s,baseline,%s,%s,%u,%llu,%llu,%llu,%llu,%llu,%.6f,%.3f,%.4f,"
                   "0,0,0,%u,%u",
                   g_cfg.label, topic, status, st.files_done,
                   (unsigned long long)st.useful_bytes,
                   (unsigned long long)st.payload_rx,
                   (unsigned long long)redundant,
                   (unsigned long long)st.wire_rx,
                   (unsigned long long)st.wire_tx,
                   elapsed, goodput, overhead, st.crc_ok, st.crc_fail);

    if (rc == 0 && st.crc_fail == 0)
        log_info("transfer of '%s' complete: %u files, %llu bytes, %.3f s, "
                 "%.1f Mbit/s", topic, st.files_done,
                 (unsigned long long)st.useful_bytes, elapsed, goodput);

    log_close();
    /* Exit status: 0 only if the transfer finished AND every CRC matched. */
    return (rc == 0 && st.crc_fail == 0) ? 0 : 1;
}
