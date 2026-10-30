/*
 * resume/client.c - fault-tolerant client WITH session management.
 *
 * Usage:  ./client <server_ip> <server_port> <topic> <output_dir> [options]
 *         Files are written to <output_dir>/<topic>/
 *
 * HOW THIS WORKS
 * --------------
 * Where Case 1 throws everything away after a fault, this client keeps a
 * durable checkpoint and resumes:
 *
 *     load <output_dir>/<topic>/.checkpoint      (if there is one)
 *     repeat:
 *         connect
 *         checkpoint?  REQ_SESSION_RESUME(session, file_idx, offset)
 *                      -> RESP_RESUME_ACK tells us where the server will start,
 *                         or refuses, in which case we discard and start fresh
 *         no checkpoint? REQ_SESSION_START(topic) -> RESP_SESSION_ID, RESP_META
 *         receive files; every 64 KiB, write the checkpoint and send REQ_ACK
 *     until RESP_DONE, or the retry cap is reached
 *
 * Two details that are easy to get wrong and are handled explicitly:
 *
 *  - Resuming a part-written file cannot use O_TRUNC, and its CRC state has to
 *    be rebuilt by re-reading the bytes already on disk (PROTOCOL.md 5.2.3).
 *    Without that the integrity check would be meaningless after any resume.
 *
 *  - A file is finalised the moment its last byte arrives, not when the next
 *    RESP_FILE_INFO shows up.  That keeps the ACK the server receives - and so
 *    the position it would resume from - as current as possible.
 *
 * Exit status is 0 only on a complete, CRC-verified transfer.
 */
#include "checkpoint.h"

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
    unsigned    ckpt_interval;
    unsigned    max_retries;
    unsigned    backoff_ms;
    int         do_fsync;
    const char *log_path;
    const char *csv_path;
    const char *label;
    int         quiet;
} g_cfg = {
    .rcvbuf        = 0,
    .nodelay       = 1,
    .ckpt_interval = DEFAULT_CKPT_INTERVAL,
    .max_retries   = DEFAULT_MAX_RETRIES,
    .backoff_ms    = DEFAULT_BACKOFF_MS_BASE,
    .do_fsync      = 0,
    .log_path      = NULL,
    .csv_path      = NULL,
    .label         = "",
    .quiet         = 0,
};

typedef enum {
    ATT_OK = 0,
    ATT_DISCONNECT = 1,
    ATT_REJECTED = 2,     /* the server refused our resume: start fresh */
    ATT_FATAL = -1
} attempt_rc;

/* Counters accumulated across every attempt. */
typedef struct {
    uint64_t wire_rx;
    uint64_t wire_tx;
    uint64_t payload_rx;    /* data bytes received, including retransmissions */
    uint64_t useful_bytes;   /* data bytes that ended up in the output files  */
    uint32_t files_done;     /* files verified complete                       */
    uint32_t crc_ok;
    uint32_t crc_fail;
    unsigned acks_sent;
    unsigned ckpts_written;
} totals_t;

/* The file currently being written. */
typedef struct {
    int      fd;
    char     name[MAX_NAME_LEN];
    uint64_t size;
    uint64_t have;          /* bytes of it on disk, counting a resume */
    uint32_t crc_expect;
    uint32_t crc_run;
    int      open;
} curfile_t;

/* All the state one attempt needs, so helpers can stay small. */
typedef struct {
    int          fd;
    const char  *topic;
    const char  *outdir;
    uint64_t     sid;
    uint32_t     file_count;
    uint32_t     next_idx;      /* index of the file we expect next */
    uint64_t     ckpt_since;    /* bytes written since the last checkpoint */
    curfile_t    cur;
    totals_t    *tot;
} att_ctx_t;

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

/* Persist the current position and tell the server about it.
 * Position semantics are (files below idx complete, `off` bytes of idx on disk). */
static int save_and_ack(att_ctx_t *a, uint32_t idx, uint64_t off)
{
    checkpoint_t cp;
    cp.session_id  = a->sid;
    snprintf(cp.topic, sizeof cp.topic, "%s", a->topic);
    cp.file_count  = a->file_count;
    cp.file_idx    = idx;
    cp.byte_offset = off;

    if (checkpoint_save(a->outdir, &cp, g_cfg.do_fsync) == 0)
        a->tot->ckpts_written++;
    else
        log_warn("could not write the checkpoint; a fault would cost more rework");

    /* The checkpoint goes to disk BEFORE the ACK goes to the server, so the
     * server can never believe the client has more than it durably has. */
    if (send_req_ack(a->fd, a->sid, idx, off, &a->tot->wire_tx) != IO_OK) {
        /* Deliberately NOT fatal.  An ACK is an optimisation: losing one only
         * means the server would resume from a slightly earlier offset.  It is
         * also normal for the very last ACK of a transfer to fail, because the
         * server may already have sent RESP_DONE and closed its side.  Letting
         * the receive loop decide whether the connection is really gone avoids
         * throwing away a transfer that has in fact just succeeded. */
        log_debug("could not send REQ_ACK (%s); continuing", strerror(errno));
    } else {
        a->tot->acks_sent++;
    }
    a->ckpt_since = 0;
    return 0;
}

/* Rebuild CRC state over the first `off` bytes of an existing output file.
 * Returns 0 on success, -1 if the file is missing or shorter than `off`. */
static int seed_crc_from_disk(const char *path, uint64_t off, uint32_t *crc_out)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) {
        log_warn("cannot reopen %s to rebuild its checksum: %s", path, strerror(errno));
        return -1;
    }
    unsigned char *buf = malloc(256 * 1024);
    if (!buf) {
        close(fd);
        return -1;
    }
    uint32_t run = CRC32_INIT;
    uint64_t left = off;
    int rc = 0;
    while (left > 0) {
        size_t want = left < 256 * 1024 ? (size_t)left : 256 * 1024;
        ssize_t n = read(fd, buf, want);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            log_warn("read %s while rebuilding its checksum: %s", path, strerror(errno));
            rc = -1;
            break;
        }
        if (n == 0) {
            log_warn("%s is shorter than the resume offset %llu; the checkpoint "
                     "cannot be trusted", path, (unsigned long long)off);
            rc = -1;
            break;
        }
        run = crc32_update(run, buf, (size_t)n);
        left -= (uint64_t)n;
    }
    free(buf);
    close(fd);
    if (rc == 0)
        *crc_out = run;
    return rc;
}

/* Close the current file and check its CRC.  Returns 0 if intact, -1 if not. */
static int finalize_file(att_ctx_t *a)
{
    curfile_t *f = &a->cur;
    if (!f->open)
        return 0;

    uint32_t got = crc32_final(f->crc_run);
    int ok = (got == f->crc_expect);
    close(f->fd);
    f->fd   = -1;
    f->open = 0;

    if (ok) {
        a->tot->crc_ok++;
    } else {
        a->tot->crc_fail++;
        log_error("CRC mismatch on %s: computed %08X, server said %08X",
                  f->name, got, f->crc_expect);
        return -1;
    }
    a->tot->files_done++;
    a->tot->useful_bytes += f->size;

    /* The file is complete, so the position advances to the next file at 0. */
    a->next_idx++;
    save_and_ack(a, a->next_idx, 0);
    return 0;
}

/* Open the output file announced by RESP_FILE_INFO.
 * `resume_off` is non-zero only for the single file a resume lands inside. */
static int open_target(att_ctx_t *a, uint64_t resume_off)
{
    curfile_t *f = &a->cur;
    char path[MAX_PATH_LEN];
    if ((size_t)snprintf(path, sizeof path, "%s/%s", a->outdir, f->name) >= sizeof path) {
        log_error("output path for %s is too long", f->name);
        return -1;
    }

    if (resume_off > 0) {
        /* Resuming inside this file: do NOT truncate, and rebuild the CRC over
         * what is already there before appending anything. */
        if (seed_crc_from_disk(path, resume_off, &f->crc_run) < 0)
            return -1;
        f->fd = open(path, O_WRONLY);
        if (f->fd < 0) {
            log_errno(LOG_ERROR, path);
            return -1;
        }
        if (lseek(f->fd, (off_t)resume_off, SEEK_SET) < 0) {
            log_errno(LOG_ERROR, "lseek to the resume offset");
            close(f->fd);
            f->fd = -1;
            return -1;
        }
        f->have = resume_off;
        log_info("resuming %s at offset %llu of %llu", f->name,
                 (unsigned long long)resume_off, (unsigned long long)f->size);
    } else {
        f->fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (f->fd < 0) {
            log_errno(LOG_ERROR, path);
            return -1;
        }
        f->crc_run = CRC32_INIT;
        f->have    = 0;
    }
    f->open = 1;
    return 0;
}

/* Perform the opening handshake, then receive until RESP_DONE or a drop. */
static attempt_rc one_attempt(att_ctx_t *a, const checkpoint_t *cp)
{
    uint64_t resume_off = 0;      /* applies to the first file we are sent */
    int      resuming   = 0;

    if (cp) {
        if (send_req_session_resume(a->fd, cp->session_id, cp->file_idx,
                                    cp->byte_offset, &a->tot->wire_tx) != IO_OK) {
            log_info("could not send REQ_SESSION_RESUME: %s", strerror(errno));
            return ATT_DISCONNECT;
        }
        a->sid        = cp->session_id;
        a->file_count = cp->file_count;
    } else {
        if (send_req_session_start(a->fd, a->topic, &a->tot->wire_tx) != IO_OK) {
            log_info("could not send REQ_SESSION_START: %s", strerror(errno));
            return ATT_DISCONNECT;
        }
    }

    int      have_meta = cp ? 1 : 0;   /* a resume does not repeat RESP_META */
    uint32_t cap = 64 * 1024;
    unsigned char *pay = malloc(cap);
    if (!pay) {
        log_error("out of memory allocating the receive buffer");
        return ATT_FATAL;
    }

    attempt_rc rc = ATT_FATAL;
    for (;;) {
        msg_hdr_t h;
        io_rc r = msg_recv_hdr(a->fd, &h, &a->tot->wire_rx);
        if (r == IO_EOF || (r == IO_ERR && (errno == ECONNRESET || errno == EPIPE
                                            || errno == ETIMEDOUT))) {
            log_info("connection dropped at file %u (%llu useful bytes so far)",
                     a->next_idx, (unsigned long long)a->tot->useful_bytes);
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
            if (!np) { log_error("out of memory growing the receive buffer"); break; }
            pay = np;
            cap = h.payload_len;
        }
        if (msg_recv_payload(a->fd, pay, h.payload_len, &a->tot->wire_rx) != IO_OK) {
            log_info("connection dropped mid-%s", msg_type_name(h.type));
            rc = ATT_DISCONNECT;
            break;
        }

        if (h.type == MSG_RESP_ERROR) {
            uint16_t code = 0;
            char text[256] = "";
            if (parse_resp_error(pay, h.payload_len, &code, text, sizeof text) == 0)
                log_error("server error %u (%s): %s", code, err_code_name(code), text);
            rc = ATT_FATAL;
            break;
        }

        if (h.type == MSG_RESP_SESSION_ID) {
            if (parse_resp_session_id(pay, h.payload_len, &a->sid, &a->file_count) < 0) {
                log_error("malformed RESP_SESSION_ID");
                break;
            }
            log_info("session %016llx opened for '%s' (%u files)",
                     (unsigned long long)a->sid, a->topic, a->file_count);
            continue;
        }

        if (h.type == MSG_RESP_RESUME_ACK) {
            uint64_t sid; uint32_t idx; uint64_t off;
            if (parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &off) < 0) {
                log_error("malformed RESP_RESUME_ACK");
                break;
            }
            if (idx == RESUME_REJECTED) {
                log_warn("server refused the resume; discarding the checkpoint "
                         "and starting the topic again");
                rc = ATT_REJECTED;
                break;
            }
            a->next_idx = idx;
            resume_off  = off;
            resuming    = 1;
            log_info("server will resume session %016llx at file %u offset %llu",
                     (unsigned long long)sid, idx, (unsigned long long)off);
            continue;
        }

        if (h.type == MSG_RESP_META) {
            uint32_t n = 0;
            if (parse_u32_only(pay, h.payload_len, &n) < 0) {
                log_error("malformed RESP_META");
                break;
            }
            if (a->file_count && n != a->file_count) {
                log_error("RESP_META says %u files but the session said %u",
                          n, a->file_count);
                break;
            }
            a->file_count = n;
            have_meta = 1;
            continue;
        }

        if (!have_meta) {
            log_error("server sent %s before RESP_META", msg_type_name(h.type));
            break;
        }

        if (h.type == MSG_RESP_CHECKPOINT) {
            uint64_t sid; uint32_t done, total;
            if (parse_resp_checkpoint(pay, h.payload_len, &sid, &done, &total) < 0) {
                log_error("malformed RESP_CHECKPOINT");
                break;
            }
            /* Purely a confirmation; our own position is authoritative. */
            log_debug("server checkpoint: %u/%u files", done, total);
            continue;
        }

        if (h.type == MSG_RESP_FILE_INFO) {
            if (a->cur.open) {
                log_error("RESP_FILE_INFO arrived while %s was still incomplete "
                          "(%llu of %llu bytes)", a->cur.name,
                          (unsigned long long)a->cur.have,
                          (unsigned long long)a->cur.size);
                break;
            }
            if (parse_resp_file_info(pay, h.payload_len, a->cur.name,
                                     sizeof a->cur.name, &a->cur.size,
                                     &a->cur.crc_expect) < 0) {
                log_error("malformed RESP_FILE_INFO");
                break;
            }
            if (name_safe_file(a->cur.name) < 0) {
                log_error("server offered an unsafe file name; refusing");
                break;
            }
            /* resume_off applies to the first file of a resumed session only. */
            uint64_t ro = resuming ? resume_off : 0;
            resuming = 0;
            if (ro > a->cur.size) {
                log_error("resume offset %llu exceeds the size of %s",
                          (unsigned long long)ro, a->cur.name);
                break;
            }
            if (open_target(a, ro) < 0) {
                /* A bad local file or an untrustworthy checkpoint: discarding
                 * the checkpoint and starting over is the safe response. */
                rc = ATT_REJECTED;
                break;
            }
            /* A zero-length file, or one already fully on disk, is complete the
             * moment it is announced. */
            if (a->cur.have >= a->cur.size) {
                if (finalize_file(a) < 0) { rc = ATT_FATAL; break; }
            }
            continue;
        }

        if (h.type == MSG_RESP_FILE_CHUNK) {
            if (!a->cur.open) {
                log_error("RESP_FILE_CHUNK arrived with no file open");
                break;
            }
            if (a->cur.have + h.payload_len > a->cur.size) {
                log_error("%s: chunk overruns the announced size", a->cur.name);
                break;
            }
            if (write_all(a->cur.fd, pay, h.payload_len) < 0) {
                log_errno(LOG_ERROR, "writing to the output file");
                break;
            }
            a->cur.crc_run     = crc32_update(a->cur.crc_run, pay, h.payload_len);
            a->cur.have       += h.payload_len;
            a->tot->payload_rx += h.payload_len;
            a->ckpt_since     += h.payload_len;

            if (a->cur.have >= a->cur.size) {
                /* Finalise as soon as the last byte lands, so the position the
                 * server would resume from is as fresh as possible. */
                if (finalize_file(a) < 0) { rc = ATT_FATAL; break; }
            } else if (a->ckpt_since >= g_cfg.ckpt_interval) {
                save_and_ack(a, a->next_idx, a->cur.have);
            }
            continue;
        }

        if (h.type == MSG_RESP_DONE) {
            if (a->cur.open) {
                log_error("RESP_DONE while %s was still incomplete", a->cur.name);
                break;
            }
            if (a->next_idx != a->file_count) {
                log_error("RESP_DONE at file %u, but the topic has %u files",
                          a->next_idx, a->file_count);
                break;
            }
            rc = ATT_OK;
            break;
        }

        log_error("unexpected message type 0x%02x (%s)", h.type, msg_type_name(h.type));
        break;
    }

    if (a->cur.open) {
        /* An incomplete file is left on disk exactly as far as it got: the
         * checkpoint records that offset and the resume continues from it. */
        close(a->cur.fd);
        a->cur.fd   = -1;
        a->cur.open = 0;
    }
    free(pay);
    return rc;
}

static void usage(const char *argv0, const opt_spec_t *specs, size_t n)
{
    fprintf(stderr,
        "usage: %s <server_ip> <server_port> <topic> <output_dir> [options]\n"
        "\n"
        "  files are written to <output_dir>/<topic>/\n"
        "  progress is checkpointed to <output_dir>/<topic>/.checkpoint and the\n"
        "  transfer resumes from there automatically after a fault\n"
        "\n"
        "options (all optional, every one has a working default):\n", argv0);
    opts_usage(specs, n, stderr);
}

int main(int argc, char **argv)
{
    const opt_spec_t specs[] = {
        { "rcvbuf",  OPT_INT,  &g_cfg.rcvbuf,        "SO_RCVBUF in bytes (default: kernel's)" },
        { "nodelay", OPT_INT,  &g_cfg.nodelay,       "1 to disable Nagle, 0 to leave it on (default 1)" },
        { "ckpt",    OPT_UINT, &g_cfg.ckpt_interval, "checkpoint every N bytes (default 65536)" },
        { "retries", OPT_UINT, &g_cfg.max_retries,   "maximum reconnection attempts (default 500)" },
        { "backoff", OPT_UINT, &g_cfg.backoff_ms,    "initial backoff in ms, doubling to 500 (default 10)" },
        { "fsync",   OPT_INT,  &g_cfg.do_fsync,      "1 to fsync each checkpoint (slower, power-loss safe)" },
        { "log",     OPT_STR,  &g_cfg.log_path,      "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path,      "append a result row to this CSV" },
        { "label",   OPT_STR,  &g_cfg.label,         "tag copied into the CSV row" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,         "log warnings and errors only" },
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
    if (g_cfg.ckpt_interval == 0) {
        fprintf(stderr, "error: --ckpt must be at least 1\n");
        return 2;
    }

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

    /* A checkpoint left by an earlier run of this client is what makes a
     * resume possible across a client restart, not just across a fault. */
    checkpoint_t cp;
    int have_cp = (checkpoint_load(outdir, &cp) == 0);
    if (have_cp && strcmp(cp.topic, topic) != 0) {
        log_warn("the checkpoint in %s is for topic '%s', not '%s'; ignoring it",
                 outdir, cp.topic, topic);
        have_cp = 0;
    }
    if (have_cp)
        log_info("found a checkpoint: session %016llx, file %u, offset %llu of "
                 "%u files", (unsigned long long)cp.session_id, cp.file_idx,
                 (unsigned long long)cp.byte_offset, cp.file_count);

    totals_t tot;
    memset(&tot, 0, sizeof tot);
    double   t0 = mono_now();
    unsigned attempts = 0, reconnects = 0, rejects = 0, resumes = 0;
    attempt_rc rc = ATT_FATAL;

    for (;;) {
        attempts++;
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

            att_ctx_t a;
            memset(&a, 0, sizeof a);
            a.fd       = fd;
            a.topic    = topic;
            a.outdir   = outdir;
            a.tot      = &tot;
            a.cur.fd   = -1;
            a.cur.crc_run = CRC32_INIT;
            if (have_cp) {
                a.next_idx   = cp.file_idx;
                a.file_count = cp.file_count;
                resumes++;
            }

            rc = one_attempt(&a, have_cp ? &cp : NULL);
            close(fd);

            if (rc == ATT_OK) {
                /* The transfer is finished, so the checkpoint has no further
                 * use and would only confuse a later run. */
                checkpoint_remove(outdir);
            } else if (rc == ATT_DISCONNECT) {
                /* Reload whatever the attempt last persisted: that, not our
                 * in-memory idea of progress, is what the next resume quotes. */
                have_cp = (checkpoint_load(outdir, &cp) == 0);
            }
        }

        if (rc == ATT_OK)
            break;
        if (rc == ATT_FATAL) {
            log_error("attempt %u failed permanently; not retrying", attempts);
            break;
        }
        if (rc == ATT_REJECTED) {
            /* Fall back to a fresh session, as PROTOCOL.md 5.2.2 prescribes. */
            rejects++;
            checkpoint_remove(outdir);
            have_cp = 0;
            if (rejects > 3) {
                log_error("the server refused to resume %u times; giving up", rejects);
                break;
            }
            log_info("starting a fresh session after a refused resume");
            continue;
        }

        reconnects++;
        if (reconnects > g_cfg.max_retries) {
            log_error("giving up after %u reconnection attempts (--retries %u)",
                      reconnects, g_cfg.max_retries);
            break;
        }
        unsigned shift = reconnects < 6 ? reconnects : 6;
        unsigned wait  = g_cfg.backoff_ms << shift;
        if (wait > DEFAULT_BACKOFF_MS_MAX)
            wait = DEFAULT_BACKOFF_MS_MAX;
        log_info("reconnecting in %u ms to resume (reconnect %u)", wait, reconnects);
        sleep_ms(wait);
    }

    double elapsed = mono_now() - t0;
    int success = (rc == ATT_OK && tot.crc_fail == 0);

    /* Redundant bytes: data that arrived but did not survive into the output.
     * With session management this should only ever be the tail of the chunk
     * that was in flight when a fault hit, plus at most one checkpoint
     * interval - not a whole attempt, as in Case 1. */
    uint64_t redundant = tot.payload_rx > tot.useful_bytes
                       ? tot.payload_rx - tot.useful_bytes : 0;
    double goodput = elapsed > 0.0 ? (double)tot.useful_bytes * 8.0 / elapsed / 1e6 : 0.0;
    double overhead = tot.useful_bytes
                    ? (double)(tot.wire_rx - tot.useful_bytes) * 100.0
                      / (double)tot.useful_bytes : 0.0;

    printf("RESULT case=resume topic=%s status=%s files=%u useful_bytes=%llu "
           "payload_bytes=%llu redundant_bytes=%llu wire_rx_bytes=%llu "
           "wire_tx_bytes=%llu elapsed_s=%.6f goodput_mbps=%.3f overhead_pct=%.4f "
           "reconnects=%u retries=%u attempts=%u resumes=%u rejects=%u faults=%u "
           "acks=%u ckpts=%u crc_ok=%u crc_fail=%u\n",
           topic, success ? "ok" : "fail", tot.files_done,
           (unsigned long long)tot.useful_bytes,
           (unsigned long long)tot.payload_rx,
           (unsigned long long)redundant,
           (unsigned long long)tot.wire_rx,
           (unsigned long long)tot.wire_tx,
           elapsed, goodput, overhead, reconnects, reconnects, attempts,
           resumes, rejects, reconnects, tot.acks_sent, tot.ckpts_written,
           tot.crc_ok, tot.crc_fail);
    fflush(stdout);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "label,case,topic,status,files,useful_bytes,payload_bytes,"
                   "redundant_bytes,wire_rx_bytes,wire_tx_bytes,elapsed_s,"
                   "goodput_mbps,overhead_pct,reconnects,retries,faults,"
                   "crc_ok,crc_fail",
                   "%s,resume,%s,%s,%u,%llu,%llu,%llu,%llu,%llu,%.6f,%.3f,"
                   "%.4f,%u,%u,%u,%u,%u",
                   g_cfg.label, topic, success ? "ok" : "fail", tot.files_done,
                   (unsigned long long)tot.useful_bytes,
                   (unsigned long long)tot.payload_rx,
                   (unsigned long long)redundant,
                   (unsigned long long)tot.wire_rx,
                   (unsigned long long)tot.wire_tx,
                   elapsed, goodput, overhead, reconnects, reconnects,
                   reconnects, tot.crc_ok, tot.crc_fail);

    if (success)
        log_info("transfer of '%s' complete: %u files, %llu useful bytes, "
                 "%u reconnect(s), %llu redundant bytes, %u checkpoint(s), "
                 "%.3f s, %.1f Mbit/s", topic, tot.files_done,
                 (unsigned long long)tot.useful_bytes, reconnects,
                 (unsigned long long)redundant, tot.ckpts_written, elapsed, goodput);

    log_close();
    return success ? 0 : 1;
}
