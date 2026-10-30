/*
 * parallel/client.c - fetches a topic over K parallel TCP
 *                                 connections, each independently resumable.
 *
 * Usage:  ./client <server_ip> <server_port> <topic> <output_dir> [K] [options]
 *         K defaults to 4, so the mandatory four-argument form works unchanged.
 *         Files are written to <output_dir>/<topic>/
 *
 * HOW THIS WORKS
 * --------------
 *   1. Connection 0 does the handshake (REQ_SESSION_START, or
 *      REQ_SESSION_RESUME if a checkpoint from an earlier run survives) and
 *      then asks for the whole file list with REQ_FILE_LIST.
 *
 *   2. The list is turned into a work plan (workplan.h): every file becomes one
 *      or more (file_idx, start, end) items, and each item belongs to exactly
 *      one connection.
 *
 *   3. Every output file is created at its final size with ftruncate() BEFORE
 *      any transfer starts.  That is what lets K connections pwrite() into the
 *      same file at different offsets with no coordination at all, and it also
 *      creates the zero-length files, which carry no work item.
 *
 *   4. K threads run, one per connection, each walking its own item list.  A
 *      fault kills one connection; that thread reconnects with REQ_ATTACH (or
 *      REQ_SESSION_RESUME for connection 0) and re-requests only the unfinished
 *      part of its current item.  The other K-1 threads never notice.
 *
 *   5. After all threads join, every file's CRC-32 is verified by reading it
 *      back.  Unlike the other cases this cannot be done while the data streams
 *      past, because a split file arrives out of order on several connections -
 *      so it is a separate pass, and its cost is reported separately as
 *      verify_s rather than being hidden inside the transfer time.
 *
 * The server still uses exactly ONE thread for this client, however large K is;
 * see the comment at the top of server.c.
 */
#include "checkpoint.h"
#include "workplan.h"

#include "../common/cliargs.h"
#include "../common/common.h"
#include "../common/crc32.h"
#include "../common/logging.h"
#include "../common/netutil.h"
#include "../common/protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
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
    uint64_t    split_threshold;
    unsigned    max_retries;
    unsigned    backoff_ms;
    int         do_fsync;
    const char *log_path;
    const char *csv_path;
    const char *label;
    int         quiet;
} g_cfg = {
    .rcvbuf          = 0,
    .nodelay         = 1,
    .ckpt_interval   = DEFAULT_CKPT_INTERVAL,
    .split_threshold = DEFAULT_SPLIT_THRESHOLD,
    .max_retries     = DEFAULT_MAX_RETRIES,
    .backoff_ms      = DEFAULT_BACKOFF_MS_BASE,
    .do_fsync        = 0,
    .log_path        = NULL,
    .csv_path        = NULL,
    .label           = "",
    .quiet           = 0,
};

/* State shared by every connection thread.  Only the checkpoint record is
 * mutable from more than one thread, and it is guarded by ckpt_lock. */
typedef struct {
    const char     *ip;
    uint16_t        port;
    const char     *topic;
    const char     *outdir;
    uint64_t        sid;
    uint32_t        file_count;
    file_meta_t    *files;
    workplan_t     *plan;

    pthread_mutex_t ckpt_lock;
    echeckpoint_t   ckpt;
} shared_t;

/* One connection thread's own state; nothing here is shared. */
typedef struct {
    shared_t *sh;
    unsigned  index;
    int       fd;
    size_t    start_item;     /* where this thread begins (from a checkpoint) */
    uint64_t  start_offset;

    uint64_t  payload_rx;     /* data bytes received on this connection    */
    uint64_t  wire_rx;
    uint64_t  wire_tx;
    unsigned  reconnects;
    unsigned  acks;
    int       failed;         /* set if this thread gave up                */
    char      err[512];
} conn_thread_t;

/* ---------------------- file list collection --------------------------- */

/* Accumulator used while RESP_FILE_LIST fragments arrive. */
typedef struct {
    file_meta_t *v;
    size_t       n;
    size_t       cap;
} filelist_t;

static int filelist_sink(void *ctx, const file_meta_t *m)
{
    filelist_t *fl = ctx;
    if (fl->n == fl->cap) {
        size_t cap = fl->cap ? fl->cap * 2 : 256;
        file_meta_t *p = realloc(fl->v, cap * sizeof *p);
        if (!p)
            return -1;
        fl->v   = p;
        fl->cap = cap;
    }
    fl->v[fl->n++] = *m;
    return 0;
}

/* --------------------------- small helpers ----------------------------- */

/* Write exactly `n` bytes at an absolute offset, looping over short writes.
 * pwrite() is what makes K connections writing one file safe: the offset is an
 * argument, so there is no shared file position to race on. */
static int pwrite_all(int fd, const void *buf, size_t n, uint64_t off)
{
    const char *p = buf;
    size_t done = 0;
    while (done < n) {
        ssize_t k = pwrite(fd, p + done, n - done, (off_t)(off + done));
        if (k < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (k == 0)
            return -1;
        done += (size_t)k;
    }
    return 0;
}

/* Record this connection's position and tell the server about it. */
static void save_progress(conn_thread_t *t, size_t item, uint64_t off,
                          uint32_t file_idx, uint64_t range_start)
{
    shared_t *sh = t->sh;

    pthread_mutex_lock(&sh->ckpt_lock);
    sh->ckpt.conn[t->index].item   = item;
    sh->ckpt.conn[t->index].offset = off;
    /* One rename() publishes all K positions together, so the set is always
     * internally consistent. */
    (void)echeckpoint_save(sh->outdir, &sh->ckpt, g_cfg.do_fsync);
    pthread_mutex_unlock(&sh->ckpt_lock);

    /* The checkpoint reaches disk before the ACK reaches the server, so the
     * server can never believe this connection has more than it durably has. */
    if (send_req_range_ack(t->fd, sh->sid, file_idx, range_start, off,
                           &t->wire_tx) == IO_OK)
        t->acks++;
    /* A failed ACK is not fatal: it only means the server would resume this
     * item from a slightly earlier offset. */
}

/* Open a connection for this thread and complete its handshake.
 * Connection 0 re-establishes the session; the others attach to it.
 * Returns 0 on success, -1 on failure. */
static int conn_open(conn_thread_t *t)
{
    shared_t *sh = t->sh;

    int fd = net_connect(sh->ip, sh->port);
    if (fd < 0)
        return -1;
    if (g_cfg.nodelay)
        net_set_nodelay(fd, 1);
    if (g_cfg.rcvbuf > 0)
        net_set_rcvbuf(fd, g_cfg.rcvbuf);

    uint64_t wire = 0;
    if (t->index == 0) {
        /* Index 0 belongs to whoever created the session, so it re-enters with
         * a resume rather than an attach.  The position it quotes is advisory:
         * the real resume happens per work item, in REQ_RANGE. */
        if (send_req_session_resume(fd, sh->sid, 0, 0, &wire) != IO_OK) {
            close(fd);
            return -1;
        }
    } else {
        if (send_req_attach(fd, sh->sid, t->index, &wire) != IO_OK) {
            close(fd);
            return -1;
        }
    }

    msg_hdr_t h;
    uint8_t pay[64];
    if (msg_recv_hdr(fd, &h, &wire) != IO_OK ||
        h.payload_len > sizeof pay ||
        msg_recv_payload(fd, pay, h.payload_len, &wire) != IO_OK) {
        close(fd);
        return -1;
    }
    t->wire_tx += wire;

    if (h.type == MSG_RESP_ERROR) {
        uint16_t code = 0;
        char text[128] = "";
        parse_resp_error(pay, h.payload_len, &code, text, sizeof text);
        snprintf(t->err, sizeof t->err, "server refused connection %u: %s (%s)",
                 t->index, text, err_code_name(code));
        log_error("%s", t->err);
        close(fd);
        return -2;                 /* permanent: do not retry */
    }
    if (t->index == 0) {
        uint64_t sid; uint32_t idx; uint64_t off;
        if (h.type != MSG_RESP_RESUME_ACK ||
            parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &off) < 0) {
            close(fd);
            return -1;
        }
        if (idx == RESUME_REJECTED) {
            snprintf(t->err, sizeof t->err,
                     "the server no longer knows session %016llx",
                     (unsigned long long)sh->sid);
            close(fd);
            return -2;
        }
    } else if (h.type != MSG_RESP_ATTACH_ACK) {
        close(fd);
        return -1;
    }

    t->fd = fd;
    return 0;
}

/* ------------------------ transferring one item ------------------------ */

typedef enum {
    ITEM_DONE = 0,
    ITEM_DROPPED = 1,     /* the connection died; reconnect and retry */
    ITEM_FATAL = -1
} item_rc;

/* Fetch [off, item->end) of item->file_idx on this connection.
 * `*off` is updated as data lands, so a retry continues from where it got to. */
static item_rc fetch_item(conn_thread_t *t, const work_item_t *item,
                          size_t item_index, uint64_t *off)
{
    shared_t *sh = t->sh;

    if (*off >= item->end)
        return ITEM_DONE;

    if (send_req_range(t->fd, sh->sid, item->file_idx, *off, item->end,
                       &t->wire_tx) != IO_OK)
        return ITEM_DROPPED;

    /* The output file was pre-created at its final size, so this just opens it. */
    char path[MAX_PATH_LEN];
    if ((size_t)snprintf(path, sizeof path, "%s/%s", sh->outdir,
                         sh->files[item->file_idx].name) >= sizeof path) {
        snprintf(t->err, sizeof t->err, "output path too long");
        return ITEM_FATAL;
    }
    int ofd = open(path, O_WRONLY);
    if (ofd < 0) {
        snprintf(t->err, sizeof t->err, "cannot open output file %s: %s",
                 sh->files[item->file_idx].name, strerror(errno));
        return ITEM_FATAL;
    }

    uint32_t cap = MSG_HEADER_SIZE + SZ_RANGE_CHUNK_PFX + 64 * 1024;
    uint8_t *pay = malloc(cap);
    if (!pay) {
        close(ofd);
        snprintf(t->err, sizeof t->err, "out of memory");
        return ITEM_FATAL;
    }

    uint64_t since_ckpt = 0;
    item_rc rc = ITEM_FATAL;

    for (;;) {
        msg_hdr_t h;
        io_rc r = msg_recv_hdr(t->fd, &h, &t->wire_rx);
        if (r == IO_EOF || (r == IO_ERR && (errno == ECONNRESET || errno == EPIPE
                                            || errno == ETIMEDOUT))) {
            log_info("conn %u: dropped in file %u at offset %llu", t->index,
                     item->file_idx, (unsigned long long)*off);
            rc = ITEM_DROPPED;
            break;
        }
        if (r != IO_OK) {
            rc = ITEM_DROPPED;
            break;
        }
        if (h.payload_len > cap) {
            uint8_t *np = realloc(pay, h.payload_len);
            if (!np) {
                snprintf(t->err, sizeof t->err, "out of memory");
                break;
            }
            pay = np;
            cap = h.payload_len;
        }
        if (msg_recv_payload(t->fd, pay, h.payload_len, &t->wire_rx) != IO_OK) {
            rc = ITEM_DROPPED;
            break;
        }

        if (h.type == MSG_RESP_RANGE_ACK) {
            uint64_t sid, start;
            uint32_t idx;
            if (parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &start) < 0 ||
                sid != sh->sid || idx != item->file_idx) {
                snprintf(t->err, sizeof t->err, "bad RESP_RANGE_ACK");
                break;
            }
            /* The server may choose to start earlier than asked - it clamps to
             * what it actually sent.  Honour it: the extra bytes simply
             * overwrite themselves at their absolute offsets. */
            if (start < *off) {
                log_debug("conn %u: server rewound file %u from %llu to %llu",
                          t->index, idx, (unsigned long long)*off,
                          (unsigned long long)start);
                *off = start;
            } else if (start > *off) {
                /* Would leave a hole; refuse rather than produce a bad file. */
                snprintf(t->err, sizeof t->err,
                         "server wanted to start file %u at %llu but this "
                         "connection only has %llu", idx,
                         (unsigned long long)start, (unsigned long long)*off);
                break;
            }
            continue;
        }

        if (h.type == MSG_RESP_RANGE_CHUNK) {
            uint32_t idx; uint64_t abs_off; const uint8_t *data; uint32_t dlen;
            if (parse_range_chunk_pfx(pay, h.payload_len, &idx, &abs_off,
                                      &data, &dlen) < 0) {
                snprintf(t->err, sizeof t->err, "malformed RESP_RANGE_CHUNK");
                break;
            }
            if (idx != item->file_idx || abs_off != *off ||
                abs_off + dlen > item->end) {
                snprintf(t->err, sizeof t->err,
                         "chunk out of place: file %u offset %llu len %u "
                         "(expected file %u offset %llu, range ends %llu)",
                         idx, (unsigned long long)abs_off, dlen,
                         item->file_idx, (unsigned long long)*off,
                         (unsigned long long)item->end);
                break;
            }
            if (pwrite_all(ofd, data, dlen, abs_off) < 0) {
                snprintf(t->err, sizeof t->err, "pwrite failed: %s", strerror(errno));
                break;
            }
            *off            += dlen;
            t->payload_rx   += dlen;
            since_ckpt      += dlen;

            if (since_ckpt >= g_cfg.ckpt_interval) {
                since_ckpt = 0;
                save_progress(t, item_index, *off, item->file_idx, item->start);
            }
            continue;
        }

        if (h.type == MSG_RESP_RANGE_DONE) {
            uint64_t sid, start, end;
            uint32_t idx;
            if (parse_sid_idx_2off(pay, h.payload_len, &sid, &idx, &start, &end) < 0) {
                snprintf(t->err, sizeof t->err, "malformed RESP_RANGE_DONE");
                break;
            }
            if (*off != item->end) {
                snprintf(t->err, sizeof t->err,
                         "RESP_RANGE_DONE at offset %llu but the range ends at %llu",
                         (unsigned long long)*off, (unsigned long long)item->end);
                break;
            }
            rc = ITEM_DONE;
            break;
        }

        if (h.type == MSG_RESP_ERROR) {
            uint16_t code = 0;
            char text[128] = "";
            parse_resp_error(pay, h.payload_len, &code, text, sizeof text);
            snprintf(t->err, sizeof t->err, "server error %s: %s",
                     err_code_name(code), text);
            break;
        }

        snprintf(t->err, sizeof t->err, "unexpected %s", msg_type_name(h.type));
        break;
    }

    free(pay);
    close(ofd);
    return rc;
}

/* --------------------------- the thread body --------------------------- */

static void *conn_thread_main(void *arg)
{
    conn_thread_t *t  = arg;
    shared_t      *sh = t->sh;
    const itemlist_t *il = &sh->plan->conn[t->index];

    for (size_t i = t->start_item; i < il->n; i++) {
        const work_item_t *item = &il->items[i];
        /* Only the item the checkpoint pointed at starts part-way through. */
        uint64_t off = (i == t->start_item && t->start_offset > item->start)
                     ? t->start_offset : item->start;
        if (off > item->end)
            off = item->end;

        for (;;) {
            if (t->fd < 0) {
                int rc = conn_open(t);
                if (rc == -2) {
                    t->failed = 1;
                    return NULL;            /* permanent */
                }
                if (rc < 0) {
                    t->reconnects++;
                    if (t->reconnects > g_cfg.max_retries) {
                        snprintf(t->err, sizeof t->err,
                                 "gave up after %u reconnection attempts",
                                 t->reconnects);
                        t->failed = 1;
                        return NULL;
                    }
                    unsigned shift = t->reconnects < 6 ? t->reconnects : 6;
                    unsigned wait  = g_cfg.backoff_ms << shift;
                    if (wait > DEFAULT_BACKOFF_MS_MAX)
                        wait = DEFAULT_BACKOFF_MS_MAX;
                    sleep_ms(wait);
                    continue;
                }
            }

            item_rc ir = fetch_item(t, item, i, &off);
            if (ir == ITEM_DONE)
                break;
            if (ir == ITEM_FATAL) {
                t->failed = 1;
                log_error("conn %u: %s", t->index, t->err);
                close(t->fd);
                t->fd = -1;
                return NULL;
            }

            /* Dropped: close, back off, reconnect, and resume this same item
             * from `off`.  The other K-1 connections are unaffected. */
            close(t->fd);
            t->fd = -1;
            t->reconnects++;
            if (t->reconnects > g_cfg.max_retries) {
                snprintf(t->err, sizeof t->err,
                         "gave up after %u reconnection attempts", t->reconnects);
                t->failed = 1;
                return NULL;
            }
            unsigned shift = t->reconnects < 6 ? t->reconnects : 6;
            unsigned wait  = g_cfg.backoff_ms << shift;
            if (wait > DEFAULT_BACKOFF_MS_MAX)
                wait = DEFAULT_BACKOFF_MS_MAX;
            sleep_ms(wait);
        }

        /* The item is complete: publish that, so a later restart skips it. */
        save_progress(t, i + 1, 0, item->file_idx, item->start);
    }

    log_debug("conn %u: all %zu item(s) complete", t->index, il->n);
    return NULL;
}

/* ------------------------- setup and teardown -------------------------- */

/* Create every output file at its final size.  Doing this before any transfer
 * starts is what lets K connections pwrite() into one file concurrently, and
 * it is also how zero-length files come into existence (they have no item). */
static int precreate_files(shared_t *sh, uint64_t *useful_bytes)
{
    *useful_bytes = 0;
    for (uint32_t i = 0; i < sh->file_count; i++) {
        if (name_safe_file(sh->files[i].name) < 0) {
            log_error("server offered an unsafe file name; refusing");
            return -1;
        }
        char path[MAX_PATH_LEN];
        if ((size_t)snprintf(path, sizeof path, "%s/%s", sh->outdir,
                             sh->files[i].name) >= sizeof path) {
            log_error("output path for %s is too long", sh->files[i].name);
            return -1;
        }
        int fd = open(path, O_WRONLY | O_CREAT, 0644);
        if (fd < 0) {
            log_errno(LOG_ERROR, path);
            return -1;
        }
        if (ftruncate(fd, (off_t)sh->files[i].size) < 0) {
            log_errno(LOG_ERROR, "ftruncate");
            close(fd);
            return -1;
        }
        close(fd);
        *useful_bytes += sh->files[i].size;
    }
    return 0;
}

/* Verify every file against the CRC the server announced.  Returns the number
 * of mismatches and reports how long the pass took. */
static uint32_t verify_files(shared_t *sh, uint32_t *ok_out, double *secs)
{
    double t0 = mono_now();
    uint32_t bad = 0, good = 0;
    for (uint32_t i = 0; i < sh->file_count; i++) {
        char path[MAX_PATH_LEN];
        snprintf(path, sizeof path, "%s/%s", sh->outdir, sh->files[i].name);
        uint32_t crc = 0;
        if (crc32_file(path, &crc) < 0) {
            log_error("cannot read %s back for verification: %s", path,
                      strerror(errno));
            bad++;
            continue;
        }
        if (crc != sh->files[i].crc32) {
            log_error("CRC mismatch on %s: computed %08X, server said %08X",
                      sh->files[i].name, crc, sh->files[i].crc32);
            bad++;
        } else {
            good++;
        }
    }
    *secs   = mono_now() - t0;
    *ok_out = good;
    return bad;
}

static void usage(const char *argv0, const opt_spec_t *specs, size_t n)
{
    fprintf(stderr,
        "usage: %s <server_ip> <server_port> <topic> <output_dir> [K] [options]\n"
        "\n"
        "  files are written to <output_dir>/<topic>/\n"
        "  K is the number of parallel connections (optional, default %d)\n"
        "\n"
        "options (all optional, every one has a working default):\n",
        argv0, DEFAULT_PARALLEL_CONNS);
    opts_usage(specs, n, stderr);
}

/* Handshake on connection 0 and collect the file list.
 * Returns the connected fd on success, -1 on failure. */
static int open_session(shared_t *sh, const echeckpoint_t *cp, filelist_t *fl,
                        uint64_t *wire_rx, uint64_t *wire_tx, int *rejected)
{
    *rejected = 0;
    int fd = net_connect(sh->ip, sh->port);
    if (fd < 0) {
        log_warn("cannot connect to %s:%u: %s", sh->ip, sh->port, strerror(errno));
        return -1;
    }
    if (g_cfg.nodelay)
        net_set_nodelay(fd, 1);
    if (g_cfg.rcvbuf > 0)
        net_set_rcvbuf(fd, g_cfg.rcvbuf);

    if (cp) {
        if (send_req_session_resume(fd, cp->session_id, 0, 0, wire_tx) != IO_OK) {
            close(fd);
            return -1;
        }
    } else {
        if (send_req_session_start(fd, sh->topic, wire_tx) != IO_OK) {
            close(fd);
            return -1;
        }
    }

    uint32_t cap = 64 * 1024;
    uint8_t *pay = malloc(cap);
    if (!pay) {
        close(fd);
        return -1;
    }

    int have_session = 0, have_list = 0, asked_list = 0;
    while (!have_list) {
        msg_hdr_t h;
        if (msg_recv_hdr(fd, &h, wire_rx) != IO_OK) {
            log_warn("connection lost during the handshake");
            goto fail;
        }
        if (h.payload_len > cap) {
            uint8_t *np = realloc(pay, h.payload_len);
            if (!np) goto fail;
            pay = np;
            cap = h.payload_len;
        }
        if (msg_recv_payload(fd, pay, h.payload_len, wire_rx) != IO_OK)
            goto fail;

        if (h.type == MSG_RESP_ERROR) {
            uint16_t code = 0;
            char text[256] = "";
            parse_resp_error(pay, h.payload_len, &code, text, sizeof text);
            log_error("server error %u (%s): %s", code, err_code_name(code), text);
            *rejected = (code == ERR_UNKNOWN_TOPIC || code == ERR_INVALID_NAME)
                      ? 2 : 1;      /* 2 = permanent, 1 = start over */
            goto fail;
        }
        if (h.type == MSG_RESP_SESSION_ID) {
            if (parse_resp_session_id(pay, h.payload_len, &sh->sid,
                                      &sh->file_count) < 0)
                goto fail;
            log_info("session %016llx opened for '%s' (%u files)",
                     (unsigned long long)sh->sid, sh->topic, sh->file_count);
            have_session = 1;
        } else if (h.type == MSG_RESP_RESUME_ACK) {
            uint64_t sid; uint32_t idx; uint64_t off;
            if (parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &off) < 0)
                goto fail;
            if (idx == RESUME_REJECTED) {
                log_warn("server refused the resume; starting a fresh session");
                *rejected = 1;
                goto fail;
            }
            sh->sid        = cp->session_id;
            sh->file_count = cp->file_count;
            log_info("resumed session %016llx for '%s'",
                     (unsigned long long)sh->sid, sh->topic);
            have_session = 1;
        } else if (h.type == MSG_RESP_FILE_LIST) {
            if (parse_resp_file_list(pay, h.payload_len, filelist_sink, fl) < 0) {
                log_error("malformed RESP_FILE_LIST");
                goto fail;
            }
            if (!(h.flags & FLAG_MORE))
                have_list = 1;
        } else {
            log_error("unexpected %s during the handshake", msg_type_name(h.type));
            goto fail;
        }

        if (have_session && !asked_list) {
            if (send_req_file_list(fd, sh->sid, wire_tx) != IO_OK)
                goto fail;
            asked_list = 1;
        }
    }

    free(pay);
    if (fl->n != sh->file_count) {
        log_error("the file list has %zu entries but the session announced %u",
                  fl->n, sh->file_count);
        close(fd);
        return -1;
    }
    return fd;

fail:
    free(pay);
    close(fd);
    return -1;
}

int main(int argc, char **argv)
{
    int k_arg = DEFAULT_PARALLEL_CONNS;
    const opt_spec_t specs[] = {
        { "rcvbuf",  OPT_INT,  &g_cfg.rcvbuf,          "SO_RCVBUF in bytes (default: kernel's)" },
        { "nodelay", OPT_INT,  &g_cfg.nodelay,         "1 to disable Nagle, 0 to leave it on (default 1)" },
        { "ckpt",    OPT_UINT, &g_cfg.ckpt_interval,   "checkpoint every N bytes (default 65536)" },
        { "split",   OPT_U64,  &g_cfg.split_threshold, "range-split files >= N bytes (default 524288)" },
        { "conns",   OPT_INT,  &k_arg,                 "parallel connections, same as the K positional" },
        { "retries", OPT_UINT, &g_cfg.max_retries,     "maximum reconnection attempts per connection (default 500)" },
        { "backoff", OPT_UINT, &g_cfg.backoff_ms,      "initial backoff in ms, doubling to 500 (default 10)" },
        { "fsync",   OPT_INT,  &g_cfg.do_fsync,        "1 to fsync each checkpoint (slower, power-loss safe)" },
        { "log",     OPT_STR,  &g_cfg.log_path,        "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path,        "append a result row to this CSV" },
        { "label",   OPT_STR,  &g_cfg.label,           "tag copied into the CSV row" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,           "log warnings and errors only" },
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

    /* K is an optional bare positional, so the mandatory four-argument form is
     * untouched.  --conns does the same job for scripts that prefer options. */
    int first_opt = 5;
    if (argc > 5 && argv[5][0] != '-') {
        char *end = NULL;
        long v = strtol(argv[5], &end, 10);
        if (!end || *end || v < 1 || v > MAX_CONNS_PER_SESSION) {
            fprintf(stderr, "error: K must be an integer in 1..%d, got '%s'\n",
                    MAX_CONNS_PER_SESSION, argv[5]);
            return 2;
        }
        k_arg = (int)v;
        first_opt = 6;
    }
    if (opts_parse(argc, argv, first_opt, specs, nspecs) < 0)
        return 2;
    if (k_arg < 1 || k_arg > MAX_CONNS_PER_SESSION) {
        fprintf(stderr, "error: --conns must be in 1..%d\n", MAX_CONNS_PER_SESSION);
        return 2;
    }
    unsigned K = (unsigned)k_arg;

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

    shared_t sh;
    memset(&sh, 0, sizeof sh);
    sh.ip = ip; sh.port = port; sh.topic = topic; sh.outdir = outdir;
    pthread_mutex_init(&sh.ckpt_lock, NULL);

    /* A checkpoint is only usable if it describes the same plan we would build
     * now: same topic, same K, same split threshold. */
    echeckpoint_t cp;
    int have_cp = (echeckpoint_load(outdir, &cp) == 0);
    if (have_cp && (strcmp(cp.topic, topic) != 0 || cp.k != K ||
                    cp.split_threshold != g_cfg.split_threshold)) {
        log_warn("the existing checkpoint describes a different plan "
                 "(topic '%s', K=%u, split=%llu); ignoring it", cp.topic, cp.k,
                 (unsigned long long)cp.split_threshold);
        have_cp = 0;
    }
    if (have_cp)
        log_info("found a checkpoint for session %016llx",
                 (unsigned long long)cp.session_id);

    double   t_start = mono_now();
    uint64_t wire_rx = 0, wire_tx = 0;
    filelist_t fl;
    memset(&fl, 0, sizeof fl);
    int conn0 = -1;

    /* Handshake, retrying a refused resume once as a fresh session. */
    for (int attempt = 0; attempt < 3; attempt++) {
        int rejected = 0;
        memset(&fl, 0, sizeof fl);
        conn0 = open_session(&sh, have_cp ? &cp : NULL, &fl, &wire_rx, &wire_tx,
                             &rejected);
        if (conn0 >= 0)
            break;
        free(fl.v);
        fl.v = NULL;
        if (rejected == 2)
            break;                       /* permanent: unknown topic */
        if (rejected == 1) {
            echeckpoint_remove(outdir);
            have_cp = 0;
            continue;
        }
        sleep_ms(100);
    }
    if (conn0 < 0) {
        log_error("could not open a session for '%s'", topic);
        free(fl.v);
        return 1;
    }
    sh.files = fl.v;

    /* Build the plan and lay out the output files. */
    workplan_t plan;
    if (workplan_build(&plan, sh.files, sh.file_count, K,
                       g_cfg.split_threshold) < 0) {
        close(conn0);
        free(sh.files);
        return 1;
    }
    sh.plan = &plan;
    workplan_log(&plan);

    uint64_t useful_bytes = 0;
    if (precreate_files(&sh, &useful_bytes) < 0) {
        close(conn0);
        workplan_free(&plan);
        free(sh.files);
        return 1;
    }

    /* Seed the shared checkpoint record. */
    sh.ckpt.session_id      = sh.sid;
    snprintf(sh.ckpt.topic, sizeof sh.ckpt.topic, "%s", topic);
    sh.ckpt.file_count      = sh.file_count;
    sh.ckpt.k               = K;
    sh.ckpt.split_threshold = g_cfg.split_threshold;
    for (unsigned j = 0; j < K; j++) {
        sh.ckpt.conn[j].item   = have_cp ? cp.conn[j].item   : 0;
        sh.ckpt.conn[j].offset = have_cp ? cp.conn[j].offset : 0;
    }

    /* Run the K connection threads. */
    double t_xfer0 = mono_now();
    conn_thread_t *th = calloc(K, sizeof *th);
    pthread_t     *tid = calloc(K, sizeof *tid);
    if (!th || !tid) {
        log_error("out of memory starting the connection threads");
        free(th); free(tid);
        close(conn0);
        workplan_free(&plan);
        free(sh.files);
        return 1;
    }

    for (unsigned j = 0; j < K; j++) {
        th[j].sh           = &sh;
        th[j].index        = j;
        th[j].fd           = (j == 0) ? conn0 : -1;   /* 0 is already open */
        th[j].start_item   = sh.ckpt.conn[j].item;
        th[j].start_offset = sh.ckpt.conn[j].offset;
    }
    unsigned started = 0;
    for (unsigned j = 0; j < K; j++) {
        if (pthread_create(&tid[j], NULL, conn_thread_main, &th[j]) != 0) {
            log_error("cannot create the thread for connection %u", j);
            th[j].failed = 1;
            break;
        }
        started++;
    }
    for (unsigned j = 0; j < started; j++)
        pthread_join(tid[j], NULL);
    double xfer_secs = mono_now() - t_xfer0;

    /* Collect per-connection totals. */
    uint64_t payload_rx = 0;
    unsigned reconnects = 0, acks = 0;
    int any_failed = 0;
    for (unsigned j = 0; j < K; j++) {
        payload_rx += th[j].payload_rx;
        wire_rx    += th[j].wire_rx;
        wire_tx    += th[j].wire_tx;
        reconnects += th[j].reconnects;
        acks       += th[j].acks;
        if (th[j].failed) {
            any_failed = 1;
            log_error("connection %u failed: %s", j,
                      th[j].err[0] ? th[j].err : "(no detail)");
        }
        if (th[j].fd >= 0)
            close(th[j].fd);
    }

    /* Verify every file.  This is a separate pass because a range-split file
     * does not arrive in order, so its CRC cannot be folded as it streams. */
    uint32_t crc_ok = 0, crc_fail = 0;
    double   verify_secs = 0.0;
    if (!any_failed)
        crc_fail = verify_files(&sh, &crc_ok, &verify_secs);

    int success = (!any_failed && crc_fail == 0);

    /* Tell the server it can retire the session, instead of waiting for the
     * idle timeout to do it. */
    if (success) {
        int fd = net_connect(ip, port);
        if (fd >= 0) {
            uint64_t w = 0;
            if (send_req_attach(fd, sh.sid, 1, &w) == IO_OK) {
                msg_hdr_t h;
                uint8_t pay[64];
                if (msg_recv_hdr(fd, &h, &w) == IO_OK &&
                    h.payload_len <= sizeof pay &&
                    msg_recv_payload(fd, pay, h.payload_len, &w) == IO_OK &&
                    h.type == MSG_RESP_ATTACH_ACK)
                    send_resp_done(fd, &w);
            }
            close(fd);
        }
        echeckpoint_remove(outdir);
    }

    double total_secs = mono_now() - t_start;
    uint64_t redundant = payload_rx > useful_bytes ? payload_rx - useful_bytes : 0;
    double goodput = total_secs > 0.0
                   ? (double)useful_bytes * 8.0 / total_secs / 1e6 : 0.0;
    double overhead = useful_bytes
                    ? (double)(wire_rx - useful_bytes) * 100.0 / (double)useful_bytes
                    : 0.0;

    printf("RESULT case=parallel topic=%s status=%s files=%u "
           "useful_bytes=%llu payload_bytes=%llu redundant_bytes=%llu "
           "wire_rx_bytes=%llu wire_tx_bytes=%llu elapsed_s=%.6f "
           "transfer_s=%.6f verify_s=%.6f goodput_mbps=%.3f overhead_pct=%.4f "
           "conns=%u items=%zu split_files=%zu reconnects=%u retries=%u "
           "faults=%u acks=%u crc_ok=%u crc_fail=%u\n",
           topic, success ? "ok" : "fail", crc_ok + crc_fail,
           (unsigned long long)useful_bytes,
           (unsigned long long)payload_rx,
           (unsigned long long)redundant,
           (unsigned long long)wire_rx,
           (unsigned long long)wire_tx,
           total_secs, xfer_secs, verify_secs, goodput, overhead,
           K, plan.total_items, plan.n_split_files, reconnects, reconnects,
           reconnects, acks, crc_ok, crc_fail);
    fflush(stdout);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "label,case,topic,status,files,useful_bytes,payload_bytes,"
                   "redundant_bytes,wire_rx_bytes,wire_tx_bytes,elapsed_s,"
                   "goodput_mbps,overhead_pct,reconnects,retries,faults,"
                   "crc_ok,crc_fail,conns,transfer_s,verify_s",
                   "%s,parallel,%s,%s,%u,%llu,%llu,%llu,%llu,%llu,"
                   "%.6f,%.3f,%.4f,%u,%u,%u,%u,%u,%u,%.6f,%.6f",
                   g_cfg.label, topic, success ? "ok" : "fail",
                   crc_ok + crc_fail,
                   (unsigned long long)useful_bytes,
                   (unsigned long long)payload_rx,
                   (unsigned long long)redundant,
                   (unsigned long long)wire_rx,
                   (unsigned long long)wire_tx,
                   total_secs, goodput, overhead, reconnects, reconnects,
                   reconnects, crc_ok, crc_fail, K, xfer_secs, verify_secs);

    if (success)
        log_info("transfer of '%s' complete over %u connection(s): %u files, "
                 "%llu useful bytes, %llu redundant, %u reconnect(s), "
                 "transfer %.3f s + verify %.3f s = %.3f s, %.1f Mbit/s",
                 topic, K, crc_ok, (unsigned long long)useful_bytes,
                 (unsigned long long)redundant, reconnects, xfer_secs,
                 verify_secs, total_secs, goodput);

    free(th);
    free(tid);
    workplan_free(&plan);
    free(sh.files);
    pthread_mutex_destroy(&sh.ckpt_lock);
    log_close();
    return success ? 0 : 1;
}
