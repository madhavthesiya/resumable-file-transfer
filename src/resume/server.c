/*
 * resume/server.c - multi-threaded server with fault injection AND
 *                        session management, so a dropped transfer resumes.
 *
 * Usage:  ./server <port> <topics_root_dir> <failure_probability> [options]
 *
 * HOW THIS WORKS
 * --------------
 * Same accept loop and same one-thread-per-client model as Case 1.  What is new
 * is a session: a server-side record of an in-progress transfer that survives
 * the connection which created it (see session.h).
 *
 *   first connection:  REQ_SESSION_START -> RESP_SESSION_ID, RESP_META, stream
 *   after a fault:     REQ_SESSION_RESUME -> RESP_RESUME_ACK, stream onwards
 *
 * While streaming, two extra things happen that Case 1 does not do:
 *
 *   - every few chunks the worker drains any REQ_ACK messages the client has
 *     sent (msg_try_recv_hdr, non-blocking).  Each one advances the session's
 *     record of what the client has confirmed.  Draining is not optional: the
 *     client sends a small ACK every 64 KiB, and if the server never read them
 *     they would fill its receive buffer until the client's send() blocked and
 *     both sides stalled.
 *
 *   - at each file boundary the worker sends RESP_CHECKPOINT, giving the client
 *     a file-level progress confirmation independent of its own byte counting.
 *
 * The resume offset is min(what the client claims, what the client has
 * confirmed), compared lexicographically over (file_idx, offset).  The server
 * never trusts a claim that runs ahead of its own record - see PROTOCOL.md
 * section 5.2.2 for why that bias is the safe one.
 */
#include "session.h"

#include "../common/cliargs.h"
#include "../common/common.h"
#include "../common/fault_inject.h"
#include "../common/logging.h"
#include "../common/netutil.h"
#include "../common/protocol.h"
#include "../common/topic_table.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* Drain the inbound ACK queue once every this many data chunks.  Four 16 KiB
 * chunks is 64 KiB, which is exactly the client's ACK cadence, so at most a
 * couple of ACKs are ever outstanding while costing one extra syscall per
 * 64 KiB instead of one per chunk. */
#define ACK_DRAIN_EVERY_CHUNKS 4

static struct {
    unsigned    chunk_size;
    unsigned    idle_sec;
    int         nodelay;
    int         sndbuf;
    unsigned    seed;
    const char *log_path;
    const char *csv_path;
    int         quiet;
    int         verbose;
} g_cfg = {
    .chunk_size = DEFAULT_CHUNK_SIZE,
    .idle_sec   = DEFAULT_SESSION_IDLE_SEC,
    .nodelay    = 1,
    .sndbuf     = 0,
    .seed       = 0,
    .log_path   = NULL,
    .csv_path   = NULL,
    .quiet      = 0,
    .verbose    = 0,
};

static const topic_table_t *g_topics;
static double               g_failure_prob;
static session_table_t      g_sessions;

static pthread_mutex_t g_tally_lock = PTHREAD_MUTEX_INITIALIZER;
static uint64_t        g_total_faults;
static uint64_t        g_total_conns;

/* --------------------------- graceful shutdown ------------------------- */

/* Set from a signal handler and read by the accept loop (and, in Case 2, by the
 * session reaper thread).  _Atomic rather than a plain `volatile`: volatile
 * stops the compiler caching the value but says nothing about visibility
 * between threads in the C11 memory model, and ThreadSanitizer is right to call
 * a volatile flag shared with another thread a data race.  A lock-free atomic
 * int is async-signal-safe, so storing to it from a handler is valid.
 * memory_order_relaxed is enough: the flag carries no data with it, and the
 * only requirement is that the loop eventually observes the store. */
static _Atomic sig_atomic_t g_stop;
static pthread_mutex_t g_workers_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_workers_cv   = PTHREAD_COND_INITIALIZER;
static int             g_workers;

static void on_signal(int sig)
{
    (void)sig;
    /* A handler must leave errno exactly as it found it: it can interrupt the
     * window between a failing syscall and the errno check that follows it,
     * and silently rewriting errno there would turn a real error into a
     * mystery.  ThreadSanitizer reports this as "signal handler spoils
     * errno". */
    int saved_errno = errno;
    atomic_store_explicit(&g_stop, 1, memory_order_relaxed);
    errno = saved_errno;
}

/* Read the flag.  Wrapped so the relaxed load appears in exactly one place. */
static int stop_requested(void)
{
    return atomic_load_explicit(&g_stop, memory_order_relaxed) != 0;
}

static void install_signal_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
}

static void worker_enter(void)
{
    pthread_mutex_lock(&g_workers_lock);
    g_workers++;
    pthread_mutex_unlock(&g_workers_lock);
}

static void worker_leave(void)
{
    pthread_mutex_lock(&g_workers_lock);
    if (--g_workers == 0)
        pthread_cond_broadcast(&g_workers_cv);
    pthread_mutex_unlock(&g_workers_lock);
}

static int wait_for_workers(int grace_sec)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += grace_sec;
    pthread_mutex_lock(&g_workers_lock);
    while (g_workers > 0)
        if (pthread_cond_timedwait(&g_workers_cv, &g_workers_lock, &deadline) == ETIMEDOUT)
            break;
    int left = g_workers;
    pthread_mutex_unlock(&g_workers_lock);
    return left;
}

/* Housekeeping thread: expires sessions whose client never came back.  This is
 * not a per-client thread - the assignment explicitly allows a separate thread
 * for a thematically different task - so the "one thread per client" rule is
 * untouched. */
static void *reaper_main(void *arg)
{
    (void)arg;
    while (!stop_requested()) {
        sleep_ms(1000);
        size_t n = session_evict_idle(&g_sessions);
        if (n)
            log_info("session reaper: evicted %zu idle session(s)", n);
    }
    return NULL;
}

/* ------------------------- per-connection context ---------------------- */

typedef struct {
    int         fd;
    char        peer[72];
    fault_rng_t fault;
    uint64_t    sid;            /* session this connection is serving */
    uint64_t    wire_tx;
    uint64_t    wire_rx;
    uint64_t    payload_tx;     /* data bytes sent on THIS connection */
    uint32_t    files_sent;
    unsigned    chunks_since_drain;
} conn_ctx_t;

typedef enum {
    STREAM_DONE  = 0,   /* the whole topic went out; RESP_DONE was sent */
    STREAM_FAULT = 1,   /* fault injected: drop without RESP_DONE       */
    STREAM_ERROR = -1
} stream_rc;

/* ---------------------------- ACK draining ----------------------------- */

/* Consume any REQ_ACK messages already waiting, updating the session record.
 * Returns 0 normally, -1 if the connection is unusable. */
static int drain_acks(conn_ctx_t *c)
{
    for (;;) {
        msg_hdr_t h;
        io_rc r = msg_try_recv_hdr(c->fd, &h, &c->wire_rx);
        if (r == IO_AGAIN)
            return 0;                 /* nothing waiting: the common case */
        if (r == IO_EOF) {
            log_debug("%s: client closed while we were streaming", c->peer);
            return -1;
        }
        if (r != IO_OK)
            return -1;

        if (h.type != MSG_REQ_ACK) {
            /* Nothing else is legal from the client mid-stream. */
            log_warn("%s: unexpected %s while streaming", c->peer,
                     msg_type_name(h.type));
            msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
            return -1;
        }
        if (h.payload_len != SZ_REQ_ACK) {
            log_warn("%s: REQ_ACK with a %u byte payload", c->peer, h.payload_len);
            msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
            return -1;
        }

        uint8_t pay[SZ_REQ_ACK];
        if (msg_recv_payload(c->fd, pay, h.payload_len, &c->wire_rx) != IO_OK)
            return -1;

        uint64_t sid; uint32_t idx; uint64_t off;
        if (parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &off) < 0) {
            log_warn("%s: malformed REQ_ACK", c->peer);
            return -1;
        }
        if (sid != c->sid) {
            log_warn("%s: REQ_ACK for session %016llx on a connection serving "
                     "%016llx", c->peer, (unsigned long long)sid,
                     (unsigned long long)c->sid);
            return -1;
        }
        session_ack(&g_sessions, sid, idx, off);
        log_debug("%s: ACK (file %u, offset %llu)", c->peer, idx,
                  (unsigned long long)off);
    }
}

/* ---------------------------- the stream ------------------------------- */

/* Send one file's RESP_FILE_INFO and the data from `from_off` onwards.
 * `from_off` is 0 for every file except the one a resume starts in. */
static stream_rc send_file_from(conn_ctx_t *c, const topic_t *tp, size_t idx,
                                uint64_t from_off, unsigned char *buf)
{
    const file_entry_t *fe = &tp->files[idx];

    char path[MAX_PATH_LEN];
    if (topic_file_path(path, sizeof path, tp, idx) < 0) {
        send_resp_error(c->fd, ERR_IO, "internal path error");
        return STREAM_ERROR;
    }

    /* RESP_FILE_INFO is sent even when from_off == fe->size and no data
     * follows: the client may not have created the file yet, and this is also
     * how a zero-length file at the resume point reaches it.  See PROTOCOL.md
     * section 5.2.2. */
    uint8_t flags = (from_off > 0) ? FLAG_RESUMED : FLAG_NONE;
    if (send_resp_file_info(c->fd, flags, fe->name, fe->size, fe->crc32,
                            &c->wire_tx) != IO_OK)
        return STREAM_ERROR;

    if (from_off >= fe->size)
        return STREAM_DONE;          /* nothing left of this file to send */

    int ffd = open(path, O_RDONLY);
    if (ffd < 0) {
        log_warn("open %s: %s", path, strerror(errno));
        send_resp_error(c->fd, ERR_IO, "cannot open %s", fe->name);
        return STREAM_ERROR;
    }

    uint64_t off = from_off;
    while (off < fe->size) {
        size_t want = fe->size - off < g_cfg.chunk_size
                    ? (size_t)(fe->size - off) : g_cfg.chunk_size;
        /* pread() takes the offset explicitly, so resuming needs no lseek and
         * the descriptor carries no shared cursor. */
        ssize_t n = pread(ffd, buf, want, (off_t)off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            log_warn("pread %s: %s", path, strerror(errno));
            close(ffd);
            send_resp_error(c->fd, ERR_IO, "read error on %s", fe->name);
            return STREAM_ERROR;
        }
        if (n == 0) {
            log_warn("%s: shorter than the cached size", path);
            close(ffd);
            send_resp_error(c->fd, ERR_IO, "file changed on disk: %s", fe->name);
            return STREAM_ERROR;
        }
        if (msg_send(c->fd, MSG_RESP_FILE_CHUNK, FLAG_NONE, buf, (uint32_t)n,
                     &c->wire_tx) != IO_OK) {
            close(ffd);
            return STREAM_ERROR;
        }
        off           += (uint64_t)n;
        c->payload_tx += (uint64_t)n;
        session_mark_sent(&g_sessions, c->sid, (uint32_t)idx, off, (uint64_t)n);

        if (++c->chunks_since_drain >= ACK_DRAIN_EVERY_CHUNKS) {
            c->chunks_since_drain = 0;
            if (drain_acks(c) < 0) {
                close(ffd);
                return STREAM_ERROR;
            }
        }

        /* THE FAULT INJECTION POINT: after a chunk, before the next one. */
        if (fault_should_trigger(&c->fault)) {
            log_warn("FAULT injected: %s session %016llx dropped in %s file "
                     "%zu (%s) at offset %llu/%llu", c->peer,
                     (unsigned long long)c->sid, tp->key, idx, fe->name,
                     (unsigned long long)off, (unsigned long long)fe->size);
            session_note_fault(&g_sessions, c->sid);
            close(ffd);
            return STREAM_FAULT;
        }
    }

    close(ffd);
    return STREAM_DONE;
}

/* Stream files [start_idx, n_files) to the client, with the first one starting
 * at start_off.  Ends with RESP_DONE, or early on a fault. */
static stream_rc stream_topic(conn_ctx_t *c, const topic_t *tp,
                              uint32_t start_idx, uint64_t start_off)
{
    unsigned char *buf = malloc(g_cfg.chunk_size);
    if (!buf) {
        log_error("out of memory allocating a %u byte chunk buffer", g_cfg.chunk_size);
        send_resp_error(c->fd, ERR_INTERNAL, "server out of memory");
        return STREAM_ERROR;
    }

    stream_rc rc = STREAM_DONE;
    for (size_t i = start_idx; i < tp->n_files; i++) {
        uint64_t from = (i == start_idx) ? start_off : 0;
        rc = send_file_from(c, tp, i, from, buf);
        if (rc != STREAM_DONE)
            goto out;
        c->files_sent++;
        /* The whole file is out, so the sent high-water mark is the start of
         * the next one (see PROTOCOL.md 5.2.1 for the position convention). */
        session_mark_sent(&g_sessions, c->sid, (uint32_t)(i + 1), 0, 0);

        /* A file-boundary confirmation, so the client has an authoritative
         * progress marker that does not depend on its own byte arithmetic. */
        if (send_resp_checkpoint(c->fd, c->sid, (uint32_t)(i + 1),
                                 (uint32_t)tp->n_files, &c->wire_tx) != IO_OK) {
            rc = STREAM_ERROR;
            goto out;
        }
        if (drain_acks(c) < 0) {
            rc = STREAM_ERROR;
            goto out;
        }
        c->chunks_since_drain = 0;
    }

    if (send_resp_done(c->fd, &c->wire_tx) != IO_OK)
        rc = STREAM_ERROR;
out:
    free(buf);
    return rc;
}

/* ------------------------- request handling ---------------------------- */

/* Read the first message of a connection.  Returns 0 on success. */
static int read_first(conn_ctx_t *c, msg_hdr_t *h, uint8_t *pay, size_t paycap)
{
    io_rc rc = msg_recv_hdr(c->fd, h, &c->wire_rx);
    if (rc != IO_OK) {
        if (rc == IO_EOF)
            log_info("%s: closed before sending a request", c->peer);
        else
            log_warn("%s: error reading the first header: %s", c->peer, strerror(errno));
        return -1;
    }
    if (h->payload_len > paycap) {
        log_warn("%s: first message payload too large (%u)", c->peer, h->payload_len);
        msg_skip_payload(c->fd, h->payload_len, &c->wire_rx);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "payload too large");
        return -1;
    }
    if (msg_recv_payload(c->fd, pay, h->payload_len, &c->wire_rx) != IO_OK) {
        log_warn("%s: truncated first message", c->peer);
        return -1;
    }
    return 0;
}

/* Handle REQ_SESSION_START: create a session and stream from the beginning. */
static stream_rc handle_start(conn_ctx_t *c, const uint8_t *pay, uint32_t len,
                              const topic_t **tp_out)
{
    char topic[MAX_KEY_LEN];
    if (parse_topic_name(pay, len, topic, sizeof topic) < 0) {
        send_resp_error(c->fd, ERR_BAD_REQUEST, "malformed REQ_SESSION_START");
        return STREAM_ERROR;
    }
    if (name_safe_topic(topic) < 0) {
        log_warn("%s: unsafe topic name rejected", c->peer);
        send_resp_error(c->fd, ERR_INVALID_NAME, "invalid topic name");
        return STREAM_ERROR;
    }
    const topic_t *tp = topic_table_lookup(g_topics, topic);
    if (!tp) {
        log_info("%s: unknown topic '%s'", c->peer, topic);
        send_resp_error(c->fd, ERR_UNKNOWN_TOPIC, "unknown topic: %s", topic);
        return STREAM_ERROR;
    }

    uint64_t sid = session_create(&g_sessions, topic, (uint32_t)tp->n_files);
    if (sid == 0) {
        send_resp_error(c->fd, ERR_INTERNAL, "cannot allocate a session");
        return STREAM_ERROR;
    }
    c->sid = sid;
    *tp_out = tp;

    log_info("%s: new session %016llx for '%s' (%zu files, %.2f MB)", c->peer,
             (unsigned long long)sid, topic, tp->n_files,
             (double)tp->total_bytes / 1048576.0);

    if (send_resp_session_id(c->fd, sid, (uint32_t)tp->n_files, &c->wire_tx) != IO_OK ||
        send_resp_meta(c->fd, (uint32_t)tp->n_files, &c->wire_tx) != IO_OK)
        return STREAM_ERROR;

    return stream_topic(c, tp, 0, 0);
}

/* Handle REQ_SESSION_RESUME: validate, clamp, acknowledge, continue. */
static stream_rc handle_resume(conn_ctx_t *c, const uint8_t *pay, uint32_t len,
                               const topic_t **tp_out)
{
    uint64_t sid; uint32_t cli_idx; uint64_t cli_off;
    if (len != SZ_REQ_RESUME ||
        parse_sid_idx_off(pay, len, &sid, &cli_idx, &cli_off) < 0) {
        send_resp_error(c->fd, ERR_BAD_REQUEST, "malformed REQ_SESSION_RESUME");
        return STREAM_ERROR;
    }

    /* Check 1: the session must still exist. */
    session_t snap;
    if (session_snapshot(&g_sessions, sid, &snap) < 0) {
        log_info("%s: resume refused, session %016llx is unknown or expired",
                 c->peer, (unsigned long long)sid);
        /* A refusal is a normal protocol outcome, not an error: the client is
         * told to start over rather than being disconnected with RESP_ERROR. */
        send_resp_resume_ack(c->fd, sid, RESUME_REJECTED, 0, &c->wire_tx);
        return STREAM_ERROR;
    }
    c->sid = sid;

    const topic_t *tp = topic_table_lookup(g_topics, snap.topic);
    if (!tp) {
        /* The catalogue is immutable for the server's lifetime, so this can
         * only mean the session is corrupt. */
        log_error("session %016llx names topic '%s', which is not in the table",
                  (unsigned long long)sid, snap.topic);
        session_note_reject(&g_sessions, sid);
        send_resp_resume_ack(c->fd, sid, RESUME_REJECTED, 0, &c->wire_tx);
        return STREAM_ERROR;
    }
    *tp_out = tp;

    /* Check 2: the index must be within the file list (equal to the count
     * means the transfer is already finished). */
    if (cli_idx > tp->n_files) {
        log_warn("%s: resume refused, file_idx %u exceeds the %zu files of '%s'",
                 c->peer, cli_idx, tp->n_files, tp->key);
        session_note_reject(&g_sessions, sid);
        send_resp_resume_ack(c->fd, sid, RESUME_REJECTED, 0, &c->wire_tx);
        return STREAM_ERROR;
    }
    /* Check 3: the offset must be within that file. */
    if (cli_idx < tp->n_files && cli_off > tp->files[cli_idx].size) {
        log_warn("%s: resume refused, offset %llu exceeds the %llu byte file %u",
                 c->peer, (unsigned long long)cli_off,
                 (unsigned long long)tp->files[cli_idx].size, cli_idx);
        session_note_reject(&g_sessions, sid);
        send_resp_resume_ack(c->fd, sid, RESUME_REJECTED, 0, &c->wire_tx);
        return STREAM_ERROR;
    }

    /* Check 4: never resume past what this session has actually transmitted.
     *
     * The client's claim comes from its own on-disk checkpoint, which it writes
     * BEFORE sending the matching REQ_ACK, so a claim is always backed by
     * durable client state and is safe to honour.  What must still be ruled out
     * is a claim that runs past data the server never sent at all, which no
     * honest client can produce - hence the clamp against sent_*, not ack_*.
     *
     * Clamping against the acknowledged position instead (as our design
     * document originally specified) looked safer but was strictly worse: the
     * server's sends land in the socket buffer and can run megabytes ahead of
     * the ACKs coming back, so at fault time ack_* trailed the client's real,
     * durable progress by the buffer size.  That threw away work the client
     * genuinely had on disk.  See README, "Deviations from the design
     * document", for the measured effect. */
    uint32_t idx = cli_idx;
    uint64_t off = cli_off;
    if (pos_before(snap.sent_file_idx, snap.sent_offset, cli_idx, cli_off)) {
        idx = snap.sent_file_idx;
        off = snap.sent_offset;
        log_warn("%s: session %016llx claimed (file %u, offset %llu) but only "
                 "(file %u, offset %llu) was ever sent; clamping to what was sent",
                 c->peer, (unsigned long long)sid, cli_idx,
                 (unsigned long long)cli_off, idx, (unsigned long long)off);
    }
    if (pos_before(snap.ack_file_idx, snap.ack_offset, idx, off))
        log_debug("%s: session %016llx resuming at (%u, %llu); the last ACK seen "
                  "was (%u, %llu), i.e. %s behind - expected, because ACKs lag "
                  "the send buffer", c->peer, (unsigned long long)sid, idx,
                  (unsigned long long)off, snap.ack_file_idx,
                  (unsigned long long)snap.ack_offset, "some way");

    session_note_resume(&g_sessions, sid);
    log_info("%s: resuming session %016llx ('%s') at file %u offset %llu "
             "(resume #%u)", c->peer, (unsigned long long)sid, tp->key, idx,
             (unsigned long long)off, snap.resumes + 1);

    if (send_resp_resume_ack(c->fd, sid, idx, off, &c->wire_tx) != IO_OK)
        return STREAM_ERROR;

    return stream_topic(c, tp, idx, off);
}

/* Serve one connection.  Returns 1 if a fault was injected on it. */
static int serve_conn(conn_ctx_t *c)
{
    double t0 = mono_now();

    msg_hdr_t h;
    uint8_t pay[2 + MAX_KEY_LEN > SZ_REQ_RESUME ? 2 + MAX_KEY_LEN : SZ_REQ_RESUME];
    if (read_first(c, &h, pay, sizeof pay) < 0)
        return 0;

    const topic_t *tp = NULL;
    stream_rc rc;
    if (h.type == MSG_REQ_SESSION_START) {
        rc = handle_start(c, pay, h.payload_len, &tp);
    } else if (h.type == MSG_REQ_SESSION_RESUME) {
        rc = handle_resume(c, pay, h.payload_len, &tp);
    } else {
        log_warn("%s: expected REQ_SESSION_START or REQ_SESSION_RESUME, got %s",
                 c->peer, msg_type_name(h.type));
        send_resp_error(c->fd, ERR_BAD_REQUEST,
                        "expected REQ_SESSION_START or REQ_SESSION_RESUME");
        return 0;
    }

    double elapsed = mono_now() - t0;
    const char *outcome = (rc == STREAM_DONE) ? "complete"
                        : (rc == STREAM_FAULT) ? "fault" : "error";

    if (rc == STREAM_DONE && tp) {
        /* The session is finished, so its record can go.  Redundant bytes are
         * simply everything this session sent beyond the topic's real size. */
        session_t snap;
        uint64_t sent = 0;
        if (session_snapshot(&g_sessions, c->sid, &snap) == 0)
            sent = snap.bytes_sent;
        uint64_t redundant = sent > tp->total_bytes ? sent - tp->total_bytes : 0;
        log_info("%s: session %016llx ('%s') COMPLETE: %llu bytes sent in total, "
                 "%llu redundant, %u resume(s), %u fault(s), last leg %.3f s",
                 c->peer, (unsigned long long)c->sid, tp->key,
                 (unsigned long long)sent, (unsigned long long)redundant,
                 snap.resumes, snap.faults, elapsed);
        session_destroy(&g_sessions, c->sid);
    } else if (rc == STREAM_FAULT) {
        /* The session record stays: that is the entire point of Case 2. */
        log_info("%s: session %016llx kept for resume after a fault (%llu bytes "
                 "on this connection, %.3f s)", c->peer,
                 (unsigned long long)c->sid, (unsigned long long)c->payload_tx, elapsed);
    }

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "role,case,topic,peer,session,outcome,files,payload_bytes,"
                   "wire_tx_bytes,elapsed_s,fault_checks,faults",
                   "server,resume,%s,%s,%016llx,%s,%u,%llu,%llu,%.6f,%llu,%llu",
                   tp ? tp->key : "?", c->peer, (unsigned long long)c->sid,
                   outcome, c->files_sent, (unsigned long long)c->payload_tx,
                   (unsigned long long)c->wire_tx, elapsed,
                   (unsigned long long)c->fault.checks,
                   (unsigned long long)c->fault.fires);

    return rc == STREAM_FAULT;
}

/* --------------------------- accept loop ------------------------------- */

static conn_ctx_t *ctx_new(int fd)
{
    conn_ctx_t *c = calloc(1, sizeof *c);
    if (!c) {
        log_error("out of memory accepting a connection");
        close(fd);
        return NULL;
    }
    c->fd = fd;
    net_peer_str(fd, c->peer, sizeof c->peer);
    fault_init(&c->fault, g_failure_prob);
    return c;
}

/* Close a healthy connection the careful way: half-close our sending side,
 * then read until the peer closes too (bounded by a short timeout), and only
 * then close.
 *
 * This matters, and getting it wrong caused a real bug during development.  In
 * Case 2 the client keeps sending small REQ_ACK messages, so when the server
 * finishes there is usually still unread client data sitting in the server's
 * receive queue.  close() on a socket with unread inbound data makes the kernel
 * send RST rather than FIN, and an RST tells the peer's stack to discard
 * whatever is in its receive buffer - including the RESP_DONE the server had
 * just sent.  The client then saw ECONNRESET instead of a completed transfer
 * and retried the whole topic, at p = 0, forever.  Draining first keeps the
 * shutdown an orderly FIN so the client reliably sees RESP_DONE. */
static void graceful_close(int fd)
{
    shutdown(fd, SHUT_WR);
    net_set_rcvtimeo(fd, 500);
    char sink[4096];
    for (;;) {
        ssize_t n = recv(fd, sink, sizeof sink, 0);
        if (n > 0)
            continue;                 /* trailing ACKs: read and discard */
        if (n < 0 && errno == EINTR)
            continue;
        break;                        /* EOF, timeout, or error: done */
    }
    close(fd);
}

static void finish_conn(conn_ctx_t *c, int faulted)
{
    pthread_mutex_lock(&g_tally_lock);
    g_total_conns++;
    g_total_faults += c->fault.fires;
    pthread_mutex_unlock(&g_tally_lock);

    if (faulted) {
        /* A fault must look like a real disconnection: no orderly ending, and
         * deliberately no drain. */
        shutdown(c->fd, SHUT_RDWR);
        close(c->fd);
    } else {
        graceful_close(c->fd);
    }
    free(c);
}

static void *worker_main(void *arg)
{
    conn_ctx_t *c = arg;
    worker_enter();
    int faulted = serve_conn(c);
    finish_conn(c, faulted);
    worker_leave();
    return NULL;
}

static int accept_loop(int lfd)
{
    while (!stop_requested()) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR)
                continue;
            if (errno == ECONNABORTED || errno == EMFILE || errno == ENFILE) {
                log_errno(LOG_WARN, "accept");
                continue;
            }
            log_errno(LOG_ERROR, "accept");
            return -1;
        }
        if (g_cfg.nodelay)
            net_set_nodelay(cfd, 1);
        if (g_cfg.sndbuf > 0)
            net_set_sndbuf(cfd, g_cfg.sndbuf);

        conn_ctx_t *c = ctx_new(cfd);
        if (!c)
            continue;

        pthread_t tid;
        int err = pthread_create(&tid, NULL, worker_main, c);
        if (err != 0) {
            log_error("pthread_create: %s; serving inline", strerror(err));
            worker_enter();
            int f = serve_conn(c);
            finish_conn(c, f);
            worker_leave();
            continue;
        }
        pthread_detach(tid);
    }
    return 0;
}

/* -------------------------------- main --------------------------------- */

static void usage(const char *argv0, const opt_spec_t *specs, size_t n)
{
    fprintf(stderr,
        "usage: %s <port> <topics_root_dir> <failure_probability> [options]\n"
        "\n"
        "  port                 TCP port to listen on\n"
        "  topics_root_dir      directory whose sub-directories are the topics\n"
        "  failure_probability  p in [0,1]: chance of dropping the connection\n"
        "                       after each data chunk (0 disables injection)\n"
        "\n"
        "options (all optional, every one has a working default):\n", argv0);
    opts_usage(specs, n, stderr);
}

int main(int argc, char **argv)
{
    const opt_spec_t specs[] = {
        { "chunk",   OPT_UINT, &g_cfg.chunk_size, "file data bytes per message (default 16384)" },
        { "idle",    OPT_UINT, &g_cfg.idle_sec,   "evict a session after this many idle seconds (default 60)" },
        { "nodelay", OPT_INT,  &g_cfg.nodelay,    "1 to disable Nagle, 0 to leave it on (default 1)" },
        { "sndbuf",  OPT_INT,  &g_cfg.sndbuf,     "SO_SNDBUF in bytes (default: kernel's)" },
        { "seed",    OPT_UINT, &g_cfg.seed,       "pin the fault-injection seed for a reproducible run" },
        { "log",     OPT_STR,  &g_cfg.log_path,   "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path,   "append a per-connection row to this CSV" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,      "log warnings and errors only" },
        { "verbose", OPT_FLAG, &g_cfg.verbose,    "log debug detail as well" },
    };
    const size_t nspecs = sizeof specs / sizeof specs[0];

    if (argc < 4) {
        usage(argv[0], specs, nspecs);
        return 2;
    }
    uint16_t port;
    if (parse_port(argv[1], &port) < 0) {
        fprintf(stderr, "error: '%s' is not a valid TCP port (1-65535)\n", argv[1]);
        return 2;
    }
    const char *root = argv[2];
    if (parse_prob(argv[3], &g_failure_prob) < 0) {
        fprintf(stderr, "error: failure_probability '%s' must be a number in [0,1]\n",
                argv[3]);
        return 2;
    }
    if (opts_parse(argc, argv, 4, specs, nspecs) < 0)
        return 2;
    if (g_cfg.chunk_size == 0 || g_cfg.chunk_size > MAX_PAYLOAD_LEN) {
        fprintf(stderr, "error: --chunk must be between 1 and %u\n", MAX_PAYLOAD_LEN);
        return 2;
    }

    log_init(g_cfg.log_path,
             g_cfg.verbose ? LOG_DEBUG : (g_cfg.quiet ? LOG_WARN : LOG_INFO));
    install_signal_handlers();
    fault_seed_init(g_cfg.seed);

    topic_table_t table;
    if (topic_table_build(&table, root, 1) < 0)
        return 1;
    topic_table_log(&table);
    g_topics = &table;

    if (session_table_init(&g_sessions, g_cfg.idle_sec) < 0) {
        topic_table_free(&table);
        return 1;
    }

    int lfd = net_listen(port, DEFAULT_LISTEN_BACKLOG);
    if (lfd < 0) {
        session_table_destroy(&g_sessions);
        topic_table_free(&table);
        return 1;
    }

    pthread_t reaper;
    int have_reaper = (pthread_create(&reaper, NULL, reaper_main, NULL) == 0);
    if (!have_reaper)
        log_warn("could not start the session reaper; idle sessions will persist");

    uint64_t chunks = (table.total_bytes + g_cfg.chunk_size - 1) / g_cfg.chunk_size;
    log_info("Part2 Case 2 server listening on port %u: p=%g per %u-byte chunk, "
             "session management ON, idle timeout %us", port, g_failure_prob,
             g_cfg.chunk_size, g_cfg.idle_sec);
    log_info("whole-dataset scale: %llu chunks, so a full pass over every topic "
             "would expect %.2f faults at this p",
             (unsigned long long)chunks, fault_expected(g_failure_prob, chunks));

    int rc = accept_loop(lfd);

    log_info("shutting down: no longer accepting connections");
    close(lfd);
    int left = wait_for_workers(5);
    if (left > 0)
        log_warn("%d worker(s) still running after the 5 s grace period; exiting anyway", left);
    else
        log_info("all workers finished");
    if (have_reaper)
        pthread_join(reaper, NULL);

    size_t live = 0, created = 0, evicted = 0;
    session_table_stats(&g_sessions, &live, &created, &evicted);
    log_info("session summary: %zu created, %zu evicted as idle, %zu still live "
             "at exit", created, evicted, live);
    log_info("fault summary: %llu connection(s) served, %llu fault(s) injected",
             (unsigned long long)g_total_conns, (unsigned long long)g_total_faults);

    session_table_destroy(&g_sessions);
    topic_table_free(&table);
    log_close();
    return rc == 0 ? 0 : 1;
}
