/*
 * parallel/server.c - session management plus K parallel
 *                                 connections per client, still with exactly
 *                                 ONE server thread per client.
 *
 * Usage:  ./server <port> <topics_root_dir> <failure_probability> [options]
 *
 * HOW THIS WORKS - the threading model is the whole point
 * ------------------------------------------------------
 * A client now opens K sockets (default 4) for one topic transfer.  The naive
 * extension of Case 2 would spawn a worker thread per accepted socket, which
 * would put K server threads to work for one client - a direct violation of the
 * assignment's "one thread serving each client".  So instead:
 *
 *   1. The listener thread accepts a socket and reads only its first message.
 *      That message always names the session (REQ_SESSION_START creates one,
 *      REQ_SESSION_RESUME and REQ_ATTACH quote an existing one).
 *
 *   2. The listener looks the session up in the owner registry:
 *        - no owner yet  -> create the registry entry and spawn ONE thread,
 *                           the session owner, which adopts this socket;
 *        - owner exists  -> push the socket (and the message already read) onto
 *                           that owner's hand-off queue and poke its eventfd.
 *      No thread is ever created for a second socket of the same client.
 *
 *   3. The owner thread runs an epoll_wait() loop over its eventfd plus every
 *      socket it owns, all in non-blocking mode.  It services whichever socket
 *      is readable or writable, reading file data with pread() so the K
 *      in-flight ranges never share a file offset.
 *
 * Thread count is therefore bounded by the number of concurrently active
 * clients, not by the number of TCP connections - for any K.
 *
 * Reading the first message on the listener thread is the one place this design
 * could stall: a client that connects and says nothing would block the accept
 * loop.  A short SO_RCVTIMEO on the fresh socket bounds that to
 * DEFAULT_HANDSHAKE_TIMEO_MS; the alternative (a small pool of handshake
 * threads) is noted in the report but was not needed at this scale.
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
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

/* The largest first message is REQ_SESSION_START, whose payload is a 2-byte
 * length plus a topic name. */
#define FIRST_MSG_CAP (2 + MAX_KEY_LEN)

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
static uint64_t        g_total_owners;

/* --------------------------- graceful shutdown ------------------------- */

/* Set from a signal handler and read by the accept loop, the reaper and every
 * session owner.  _Atomic rather than plain `volatile` for the reason spelled
 * out in the other servers: volatile says nothing about visibility between
 * threads in the C11 memory model. */
static _Atomic sig_atomic_t g_stop;

static void on_signal(int sig)
{
    (void)sig;
    int saved_errno = errno;      /* a handler must not disturb errno */
    atomic_store_explicit(&g_stop, 1, memory_order_relaxed);
    errno = saved_errno;
}

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
    sa.sa_flags = 0;              /* no SA_RESTART: accept() must see EINTR */
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);
}

static pthread_mutex_t g_workers_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_workers_cv   = PTHREAD_COND_INITIALIZER;
static int             g_workers;

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

/* ======================== the owner registry =========================== */
/*
 * Maps a session id to the thread that owns it, and carries sockets from the
 * listener to that thread.  This is the mechanism that keeps "one thread per
 * client" true when a client holds K sockets.
 */

/* A socket that has already completed its handshake on the listener thread,
 * together with the connection index the client gave it.  The handshake is
 * done by the listener precisely so that this hand-off stays this small: the
 * owner thread only ever deals with data-phase messages. */
typedef struct {
    int      fd;
    uint32_t conn_index;
} handoff_t;

typedef struct owner {
    uint64_t        sid;
    int             wake_fd;        /* eventfd the owner blocks on       */
    handoff_t       queue[MAX_CONNS_PER_SESSION];
    int             n_queued;
    int             live;           /* cleared when the owner is exiting */
    struct owner   *next;
} owner_t;

static owner_t        *g_owners;
static pthread_mutex_t g_owners_lock = PTHREAD_MUTEX_INITIALIZER;

/* Caller must hold g_owners_lock. */
static owner_t *owner_find_locked(uint64_t sid)
{
    for (owner_t *o = g_owners; o; o = o->next)
        if (o->sid == sid && o->live)
            return o;
    return NULL;
}

/* Hand a socket to an existing owner.  Returns 0 if it was queued, -1 if the
 * owner's queue is full (the caller then rejects the connection). */
static int owner_push(owner_t *o, const handoff_t *item)
{
    if (o->n_queued >= MAX_CONNS_PER_SESSION)
        return -1;
    o->queue[o->n_queued++] = *item;
    /* Waking the owner is a single 8-byte write; eventfd counts, so several
     * pokes before the owner runs collapse into one wake-up. */
    uint64_t one = 1;
    if (write(o->wake_fd, &one, sizeof one) != (ssize_t)sizeof one)
        log_errno(LOG_WARN, "write to the owner eventfd");
    return 0;
}

/* Drain the queue into `out` (capacity MAX_CONNS_PER_SESSION).  Returns how
 * many items were taken. */
static int owner_drain(owner_t *o, handoff_t *out)
{
    pthread_mutex_lock(&g_owners_lock);
    int n = o->n_queued;
    for (int i = 0; i < n; i++)
        out[i] = o->queue[i];
    o->n_queued = 0;
    pthread_mutex_unlock(&g_owners_lock);
    return n;
}

/* Unregister and free an owner once its thread is finished with it. */
static void owner_retire(owner_t *dead)
{
    pthread_mutex_lock(&g_owners_lock);
    dead->live = 0;
    owner_t **link = &g_owners;
    while (*link) {
        if (*link == dead) {
            *link = dead->next;
            break;
        }
        link = &(*link)->next;
    }
    /* Anything queued after the owner decided to exit must not be leaked. */
    for (int i = 0; i < dead->n_queued; i++)
        close(dead->queue[i].fd);
    pthread_mutex_unlock(&g_owners_lock);
    close(dead->wake_fd);
    free(dead);
}

/* ==================== one client's multiplexed state =================== */
/*
 * Everything below runs inside a single session-owner thread.  Because there
 * is exactly one such thread per client, none of it needs locking: the only
 * shared structures it touches are the session table and the owner registry,
 * and both have their own mutex.
 */

typedef enum {
    JOB_NONE = 0,       /* waiting for a request from this connection */
    JOB_FILE_LIST,      /* streaming RESP_FILE_LIST fragments         */
    JOB_RANGE           /* streaming RESP_RANGE_CHUNKs                */
} job_kind_t;

typedef struct {
    int        fd;              /* -1 when this slot is unused            */
    uint32_t   conn_index;      /* the client's own numbering             */

    /* Inbound: one message at a time.  Every client-to-server message in this
     * case is small, so a fixed buffer is enough and no growth is needed. */
    uint8_t    in[MSG_HEADER_SIZE + FIRST_MSG_CAP];
    size_t     in_have;
    int        in_hdr_done;
    msg_hdr_t  hdr;

    /* Outbound: exactly one framed message at a time, flushed across as many
     * writes as the socket needs. */
    uint8_t   *out;
    size_t     out_cap;
    size_t     out_len;
    size_t     out_sent;

    /* The job that generates outbound messages. */
    job_kind_t job;
    uint32_t   list_cursor;     /* JOB_FILE_LIST: next file to describe    */
    uint32_t   file_idx;        /* JOB_RANGE                               */
    uint64_t   range_start;
    uint64_t   range_end;
    uint64_t   next_off;        /* next absolute offset to read            */
    int        ffd;             /* open file for the current range         */
    uint64_t   chunk_in_flight; /* data bytes sitting in `out`, 0 if none  */

    uint64_t   payload_tx;
    int        want_out;        /* currently registered for EPOLLOUT       */
} econn_t;

typedef struct {
    uint64_t       sid;
    owner_t       *reg;
    const topic_t *tp;
    int            ep;
    econn_t        conns[MAX_CONNS_PER_SESSION];
    int            n_live;
    fault_rng_t    fault;
    char           peer[72];
    uint64_t       wire_tx;
    uint64_t       wire_rx;
    uint32_t       faults;
    double         t0;
    int            finished;     /* the client signalled it has everything */
} owner_ctx_t;

/* Sentinel in epoll_event.data.u32 marking the hand-off eventfd. */
#define EV_WAKE 0xFFFFFFFFu

static void ec_close(owner_ctx_t *ctx, econn_t *c, const char *why, int abrupt)
{
    if (c->fd < 0)
        return;
    log_debug("%s: conn %u closed (%s)", ctx->peer, c->conn_index, why);
    epoll_ctl(ctx->ep, EPOLL_CTL_DEL, c->fd, NULL);
    /* An injected fault must look like a real disconnection, so it skips the
     * orderly half-close used for a connection that finished its work. */
    shutdown(c->fd, abrupt ? SHUT_RDWR : SHUT_WR);
    close(c->fd);
    c->fd = -1;
    if (c->ffd >= 0) {
        close(c->ffd);
        c->ffd = -1;
    }
    free(c->out);
    c->out = NULL;
    c->job = JOB_NONE;
    ctx->n_live--;
}

/* Register (or re-register) the connection for the events it currently wants.
 * EPOLLOUT is only requested while there is something to write, so an idle
 * connection does not spin. */
static void ec_arm(owner_ctx_t *ctx, econn_t *c)
{
    if (c->fd < 0)
        return;
    int want_out = (c->out_sent < c->out_len) || (c->job != JOB_NONE);
    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events   = EPOLLIN | EPOLLRDHUP | (want_out ? EPOLLOUT : 0);
    ev.data.u32 = (uint32_t)(c - ctx->conns);
    if (epoll_ctl(ctx->ep, EPOLL_CTL_MOD, c->fd, &ev) < 0)
        log_errno(LOG_WARN, "epoll_ctl MOD");
    c->want_out = want_out;
}

/* ------------------------- producing output ---------------------------- */

/* Frame a message that is already sitting at out + MSG_HEADER_SIZE. */
static void ec_frame(econn_t *c, uint8_t type, uint8_t flags, size_t body_len)
{
    c->out[0] = type;
    c->out[1] = flags;
    c->out[2] = (uint8_t)(body_len >> 24);
    c->out[3] = (uint8_t)(body_len >> 16);
    c->out[4] = (uint8_t)(body_len >> 8);
    c->out[5] = (uint8_t)(body_len);
    c->out_len  = MSG_HEADER_SIZE + body_len;
    c->out_sent = 0;
}

/* Build the next RESP_FILE_LIST fragment.  Returns 1 if one was produced. */
static int produce_file_list(owner_ctx_t *ctx, econn_t *c)
{
    const topic_t *tp = ctx->tp;
    uint8_t *body = c->out + MSG_HEADER_SIZE;
    size_t   cap  = c->out_cap - MSG_HEADER_SIZE;

    pb_t w;
    pb_init(&w, body, cap);
    pb_u32(&w, 0);                       /* entry count, patched below */

    uint32_t count = 0;
    while (c->list_cursor < tp->n_files) {
        const file_entry_t *fe = &tp->files[c->list_cursor];
        size_t nlen = strlen(fe->name);
        if (w.len + 2 + nlen + 8 + 4 > cap)
            break;                       /* this fragment is full */
        pb_u16(&w, (uint16_t)nlen);
        pb_bytes(&w, fe->name, nlen);
        pb_u64(&w, fe->size);
        pb_u32(&w, fe->crc32);
        count++;
        c->list_cursor++;
    }
    if (pb_ok(&w) < 0) {
        log_error("file list fragment overflowed its buffer");
        return -1;
    }
    /* Patch the count now that it is known. */
    body[0] = (uint8_t)(count >> 24); body[1] = (uint8_t)(count >> 16);
    body[2] = (uint8_t)(count >> 8);  body[3] = (uint8_t)(count);

    int more = (c->list_cursor < tp->n_files);
    ec_frame(c, MSG_RESP_FILE_LIST, more ? FLAG_MORE : FLAG_NONE, w.len);
    if (!more)
        c->job = JOB_NONE;               /* this was the last fragment */
    return 1;
}

/* Build the next RESP_RANGE_CHUNK, or RESP_RANGE_DONE when the range is
 * finished.  Returns 1 if a message was produced, -1 on error. */
static int produce_range(owner_ctx_t *ctx, econn_t *c)
{
    if (c->next_off >= c->range_end) {
        /* The range is complete: tell the client and go idle. */
        uint8_t *body = c->out + MSG_HEADER_SIZE;
        pb_t w;
        pb_init(&w, body, c->out_cap - MSG_HEADER_SIZE);
        pb_u64(&w, ctx->sid);
        pb_u32(&w, c->file_idx);
        pb_u64(&w, c->range_start);
        pb_u64(&w, c->range_end);
        ec_frame(c, MSG_RESP_RANGE_DONE, FLAG_NONE, w.len);
        if (c->ffd >= 0) {
            close(c->ffd);
            c->ffd = -1;
        }
        c->job = JOB_NONE;
        return 1;
    }

    uint64_t left = c->range_end - c->next_off;
    size_t   want = left < g_cfg.chunk_size ? (size_t)left : g_cfg.chunk_size;

    /* pread() carries the offset, so the K ranges in flight for this client
     * never contend on a shared file cursor - which is what makes one thread
     * serving K connections correct without any per-file locking. */
    uint8_t *data = c->out + MSG_HEADER_SIZE + SZ_RANGE_CHUNK_PFX;
    ssize_t  n;
    for (;;) {
        n = pread(c->ffd, data, want, (off_t)c->next_off);
        if (n >= 0 || errno != EINTR)
            break;
    }
    if (n < 0) {
        log_warn("pread on file %u: %s", c->file_idx, strerror(errno));
        return -1;
    }
    if (n == 0) {
        log_warn("file %u is shorter than its catalogued size", c->file_idx);
        return -1;
    }

    uint8_t *body = c->out + MSG_HEADER_SIZE;
    pb_t w;
    pb_init(&w, body, SZ_RANGE_CHUNK_PFX);
    pb_u32(&w, c->file_idx);
    pb_u64(&w, c->next_off);
    ec_frame(c, MSG_RESP_RANGE_CHUNK, FLAG_NONE, SZ_RANGE_CHUNK_PFX + (size_t)n);
    c->chunk_in_flight = (uint64_t)n;
    return 1;
}

static int ec_produce(owner_ctx_t *ctx, econn_t *c)
{
    switch (c->job) {
    case JOB_FILE_LIST: return produce_file_list(ctx, c);
    case JOB_RANGE:     return produce_range(ctx, c);
    case JOB_NONE:      return 0;
    }
    return 0;
}

/* ------------------------- flushing output ----------------------------- */

/* Push as much of `out` as the socket will take.
 * Returns 1 if the buffer is now empty, 0 if it is partially sent (the socket
 * is full), -1 if the connection is dead. */
static int ec_flush(owner_ctx_t *ctx, econn_t *c)
{
    while (c->out_sent < c->out_len) {
        ssize_t n = send(c->fd, c->out + c->out_sent, c->out_len - c->out_sent,
                         MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return 0;              /* wait for EPOLLOUT */
            return -1;
        }
        c->out_sent += (size_t)n;
        ctx->wire_tx += (uint64_t)n;
    }
    return 1;
}

/* Called once a data chunk has been fully handed to the kernel: advance the
 * offsets, update the session, and then make the fault decision. */
static int ec_chunk_completed(owner_ctx_t *ctx, econn_t *c)
{
    uint64_t n = c->chunk_in_flight;
    c->chunk_in_flight = 0;
    if (n == 0)
        return 0;

    c->next_off   += n;
    c->payload_tx += n;
    session_range_sent(&g_sessions, ctx->sid, c->conn_index, c->next_off, n);

    /* THE FAULT INJECTION POINT.  Evaluated per emitted data chunk, exactly as
     * in the other Part II cases, so p keeps the same meaning.  Only the one
     * connection that emitted the chunk is dropped: the client's other K-1
     * connections keep streaming, which is precisely the resilience the
     * Enhanced case is meant to demonstrate. */
    if (fault_should_trigger(&ctx->fault)) {
        log_warn("FAULT injected: %s session %016llx conn %u dropped in file %u "
                 "at offset %llu (range %llu..%llu)", ctx->peer,
                 (unsigned long long)ctx->sid, c->conn_index, c->file_idx,
                 (unsigned long long)c->next_off,
                 (unsigned long long)c->range_start,
                 (unsigned long long)c->range_end);
        ctx->faults++;
        session_note_fault(&g_sessions, ctx->sid);
        ec_close(ctx, c, "fault injected", 1);
        return -1;                      /* this connection is gone */
    }
    return 0;
}

/* Drive a connection's output as far as it will go this turn.
 * Returns 0 normally, -1 if the connection was closed. */
static int ec_pump(owner_ctx_t *ctx, econn_t *c)
{
    /* Bounded so one connection cannot monopolise the owner thread while its
     * siblings wait: at 16 KiB a chunk this is 512 KiB per epoll turn. */
    const int MAX_MSGS_PER_TURN = 32;

    for (int k = 0; k < MAX_MSGS_PER_TURN; k++) {
        if (c->out_sent < c->out_len) {
            int r = ec_flush(ctx, c);
            if (r < 0) {
                ec_close(ctx, c, "send failed", 1);
                return -1;
            }
            if (r == 0)
                return 0;               /* socket full; wait for EPOLLOUT */
            /* Fully sent.  If that was a data chunk, account for it now. */
            if (ec_chunk_completed(ctx, c) < 0)
                return -1;
            continue;
        }
        if (c->job == JOB_NONE)
            return 0;                   /* nothing more to generate */
        int p = ec_produce(ctx, c);
        if (p < 0) {
            send_resp_error(c->fd, ERR_IO, "server read error");
            ec_close(ctx, c, "produce failed", 1);
            return -1;
        }
        if (p == 0)
            return 0;
    }
    return 0;
}

/* ------------------------- handling input ------------------------------ */

/* Act on one complete inbound message.  Only three types are legal once a
 * connection has finished its handshake; anything else is a protocol error.
 * Returns 0 normally, -1 if the connection must be dropped. */
static int ec_handle_msg(owner_ctx_t *ctx, econn_t *c)
{
    const uint8_t *pay = c->in + MSG_HEADER_SIZE;
    uint32_t len = c->hdr.payload_len;

    switch (c->hdr.type) {
    case MSG_REQ_FILE_LIST: {
        uint64_t sid;
        if (len != SZ_REQ_FILE_LIST || parse_sid_only(pay, len, &sid) < 0 ||
            sid != ctx->sid) {
            log_warn("%s: bad REQ_FILE_LIST on conn %u", ctx->peer, c->conn_index);
            send_resp_error(c->fd, ERR_BAD_REQUEST, "bad REQ_FILE_LIST");
            return -1;
        }
        c->job         = JOB_FILE_LIST;
        c->list_cursor = 0;
        log_debug("%s: conn %u requested the file list (%zu files)", ctx->peer,
                  c->conn_index, ctx->tp->n_files);
        return 0;
    }

    case MSG_REQ_RANGE: {
        uint64_t sid, start, end;
        uint32_t idx;
        if (len != SZ_REQ_RANGE ||
            parse_sid_idx_2off(pay, len, &sid, &idx, &start, &end) < 0 ||
            sid != ctx->sid) {
            log_warn("%s: bad REQ_RANGE on conn %u", ctx->peer, c->conn_index);
            send_resp_error(c->fd, ERR_BAD_REQUEST, "bad REQ_RANGE");
            return -1;
        }
        if (idx >= ctx->tp->n_files || end > ctx->tp->files[idx].size ||
            start > end) {
            log_warn("%s: conn %u asked for an impossible range: file %u "
                     "[%llu,%llu) of a %llu byte file", ctx->peer, c->conn_index,
                     idx, (unsigned long long)start, (unsigned long long)end,
                     idx < ctx->tp->n_files
                         ? (unsigned long long)ctx->tp->files[idx].size : 0ull);
            send_resp_error(c->fd, ERR_BAD_RANGE, "range outside the file");
            return -1;
        }

        /* The clamp: a re-issued request for the same item after a fault picks
         * up where that item left off rather than restarting the range. */
        uint64_t begin_at = start;
        session_range_begin(&g_sessions, ctx->sid, c->conn_index, idx, start,
                            end, &begin_at);

        if (c->ffd >= 0)
            close(c->ffd);
        char path[MAX_PATH_LEN];
        if (topic_file_path(path, sizeof path, ctx->tp, idx) < 0 ||
            (c->ffd = open(path, O_RDONLY)) < 0) {
            log_warn("%s: cannot open file %u: %s", ctx->peer, idx, strerror(errno));
            send_resp_error(c->fd, ERR_IO, "cannot open the requested file");
            return -1;
        }

        c->file_idx    = idx;
        c->range_start = start;
        c->range_end   = end;
        c->next_off    = begin_at;
        c->job         = JOB_RANGE;

        /* RESP_RANGE_ACK goes out first, telling the client where the data will
         * actually start.  It is produced through the same buffered path as
         * everything else, so it cannot block the owner thread. */
        uint8_t *body = c->out + MSG_HEADER_SIZE;
        pb_t w;
        pb_init(&w, body, c->out_cap - MSG_HEADER_SIZE);
        pb_u64(&w, ctx->sid);
        pb_u32(&w, idx);
        pb_u64(&w, begin_at);
        ec_frame(c, MSG_RESP_RANGE_ACK, FLAG_NONE, w.len);

        log_debug("%s: conn %u range file %u [%llu,%llu) starting at %llu",
                  ctx->peer, c->conn_index, idx, (unsigned long long)start,
                  (unsigned long long)end, (unsigned long long)begin_at);
        return 0;
    }

    case MSG_REQ_RANGE_ACK: {
        uint64_t sid, rstart, abs_off;
        uint32_t idx;
        if (len != SZ_REQ_RANGE_ACK ||
            parse_sid_idx_2off(pay, len, &sid, &idx, &rstart, &abs_off) < 0 ||
            sid != ctx->sid) {
            log_warn("%s: bad REQ_RANGE_ACK on conn %u", ctx->peer, c->conn_index);
            return -1;
        }
        session_range_acked(&g_sessions, ctx->sid, c->conn_index, idx, rstart,
                            abs_off);
        return 0;
    }

    case MSG_RESP_DONE:
        /* The client uses RESP_DONE's type value as its "I have everything"
         * signal, so the owner can retire the session promptly instead of
         * waiting for the idle timeout. */
        log_info("%s: session %016llx reported complete by the client",
                 ctx->peer, (unsigned long long)ctx->sid);
        ctx->finished = 1;
        return -1;

    default:
        log_warn("%s: unexpected %s on conn %u", ctx->peer,
                 msg_type_name(c->hdr.type), c->conn_index);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "unexpected message");
        return -1;
    }
}

/* Read whatever is available and act on every complete message in it.
 * Returns 0 normally, -1 if the connection was closed. */
static int ec_on_readable(owner_ctx_t *ctx, econn_t *c)
{
    for (;;) {
        /* Phase 1: the 6-byte header. */
        if (!c->in_hdr_done) {
            size_t need = MSG_HEADER_SIZE - c->in_have;
            ssize_t n = recv(c->fd, c->in + c->in_have, need, 0);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return 0;
                ec_close(ctx, c, "recv failed", 1);
                return -1;
            }
            if (n == 0) {
                ec_close(ctx, c, "peer closed", 0);
                return -1;
            }
            ctx->wire_rx += (uint64_t)n;
            c->in_have   += (size_t)n;
            if (c->in_have < MSG_HEADER_SIZE)
                return 0;                /* partial header; come back later */

            c->hdr.type        = c->in[0];
            c->hdr.flags       = c->in[1];
            c->hdr.payload_len = ((uint32_t)c->in[2] << 24) |
                                 ((uint32_t)c->in[3] << 16) |
                                 ((uint32_t)c->in[4] << 8)  | (uint32_t)c->in[5];
            if (c->hdr.payload_len > FIRST_MSG_CAP) {
                /* Every legal client message in this case is tiny, so an
                 * oversized declaration is a protocol error, not something to
                 * allocate for. */
                log_warn("%s: conn %u declared a %u byte payload", ctx->peer,
                         c->conn_index, c->hdr.payload_len);
                ec_close(ctx, c, "oversized payload", 1);
                return -1;
            }
            c->in_hdr_done = 1;
            c->in_have     = 0;
        }

        /* Phase 2: the payload. */
        if (c->hdr.payload_len > 0) {
            size_t need = c->hdr.payload_len - c->in_have;
            ssize_t n = recv(c->fd, c->in + MSG_HEADER_SIZE + c->in_have, need, 0);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    return 0;
                ec_close(ctx, c, "recv failed", 1);
                return -1;
            }
            if (n == 0) {
                ec_close(ctx, c, "peer closed mid-message", 1);
                return -1;
            }
            ctx->wire_rx += (uint64_t)n;
            c->in_have   += (size_t)n;
            if (c->in_have < c->hdr.payload_len)
                return 0;                /* partial payload */
        }

        /* A whole message is in hand. */
        int rc = ec_handle_msg(ctx, c);
        c->in_hdr_done = 0;
        c->in_have     = 0;
        if (rc < 0) {
            if (c->fd >= 0)
                ec_close(ctx, c, "request rejected", 1);
            return -1;
        }
        /* Loop: several small requests can arrive in one read. */
    }
}

/* ------------------------- adopting a socket --------------------------- */

/* Take ownership of a handed-off socket.  Returns 0 on success. */
static int ec_adopt(owner_ctx_t *ctx, int fd, uint32_t conn_index)
{
    if (conn_index >= MAX_CONNS_PER_SESSION) {
        log_warn("%s: refusing conn index %u (max %d)", ctx->peer, conn_index,
                 MAX_CONNS_PER_SESSION - 1);
        close(fd);
        return -1;
    }
    econn_t *c = &ctx->conns[conn_index];
    if (c->fd >= 0) {
        /* The client reconnected this index before we noticed the old socket
         * was dead.  The new one wins; the stale one is dropped. */
        log_info("%s: conn %u reconnected; dropping the previous socket",
                 ctx->peer, conn_index);
        ec_close(ctx, c, "superseded by a reconnect", 1);
    }

    memset(c, 0, sizeof *c);
    c->fd         = fd;
    c->conn_index = conn_index;
    c->ffd        = -1;
    c->out_cap    = MSG_HEADER_SIZE + SZ_RANGE_CHUNK_PFX + g_cfg.chunk_size;
    c->out        = malloc(c->out_cap);
    if (!c->out) {
        log_error("out of memory adopting conn %u", conn_index);
        close(fd);
        c->fd = -1;
        return -1;
    }

    /* Non-blocking from here on: one thread serves all K sockets, so it must
     * never block on any single one of them. */
    net_set_nonblock(fd, 1);
    /* The handshake timeouts were only for the listener's blocking reads. */
    net_set_rcvtimeo(fd, 0);
    if (g_cfg.nodelay)
        net_set_nodelay(fd, 1);
    if (g_cfg.sndbuf > 0)
        net_set_sndbuf(fd, g_cfg.sndbuf);

    struct epoll_event ev;
    memset(&ev, 0, sizeof ev);
    ev.events   = EPOLLIN | EPOLLRDHUP;
    ev.data.u32 = conn_index;
    if (epoll_ctl(ctx->ep, EPOLL_CTL_ADD, fd, &ev) < 0) {
        log_errno(LOG_ERROR, "epoll_ctl ADD");
        free(c->out);
        c->out = NULL;
        close(fd);
        c->fd = -1;
        return -1;
    }
    ctx->n_live++;
    log_debug("%s: adopted conn %u (now %d live)", ctx->peer, conn_index,
              ctx->n_live);
    return 0;
}

/* ======================= the session owner thread ====================== */
/*
 * Exactly one of these runs per client, for any K.  It owns every socket that
 * client has opened for its session and multiplexes them with epoll.
 */

typedef struct {
    uint64_t sid;
    owner_t *reg;
    int      first_fd;
    uint32_t first_conn_index;
    char     peer[72];
} owner_start_t;

static void *owner_main(void *arg)
{
    owner_start_t *start = arg;

    owner_ctx_t ctx;
    memset(&ctx, 0, sizeof ctx);
    ctx.sid = start->sid;
    ctx.reg = start->reg;
    ctx.t0  = mono_now();
    snprintf(ctx.peer, sizeof ctx.peer, "%s", start->peer);
    for (int i = 0; i < MAX_CONNS_PER_SESSION; i++) {
        ctx.conns[i].fd  = -1;
        ctx.conns[i].ffd = -1;
    }
    fault_init(&ctx.fault, g_failure_prob);

    worker_enter();
    pthread_mutex_lock(&g_tally_lock);
    g_total_owners++;
    pthread_mutex_unlock(&g_tally_lock);

    /* Resolve the topic once; the catalogue is immutable so this stays valid. */
    session_t snap;
    if (session_snapshot(&g_sessions, ctx.sid, &snap) < 0) {
        log_warn("owner for session %016llx started but the session is gone",
                 (unsigned long long)ctx.sid);
        close(start->first_fd);
        goto out_no_ep;
    }
    ctx.tp = topic_table_lookup(g_topics, snap.topic);
    if (!ctx.tp) {
        log_error("session %016llx names unknown topic '%s'",
                  (unsigned long long)ctx.sid, snap.topic);
        close(start->first_fd);
        goto out_no_ep;
    }

    ctx.ep = epoll_create1(0);
    if (ctx.ep < 0) {
        log_errno(LOG_ERROR, "epoll_create1");
        close(start->first_fd);
        goto out_no_ep;
    }

    /* The hand-off eventfd is just another descriptor in the same epoll set,
     * so a new connection arriving and data becoming ready are handled by the
     * same loop rather than needing a separate wake-up mechanism. */
    struct epoll_event wev;
    memset(&wev, 0, sizeof wev);
    wev.events   = EPOLLIN;
    wev.data.u32 = EV_WAKE;
    if (epoll_ctl(ctx.ep, EPOLL_CTL_ADD, ctx.reg->wake_fd, &wev) < 0) {
        log_errno(LOG_ERROR, "epoll_ctl ADD eventfd");
        close(start->first_fd);
        goto out;
    }

    ec_adopt(&ctx, start->first_fd, start->first_conn_index);

    double last_activity = mono_now();

    while (!stop_requested()) {
        struct epoll_event evs[MAX_CONNS_PER_SESSION + 1];
        int n = epoll_wait(ctx.ep, evs, MAX_CONNS_PER_SESSION + 1, 1000);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            log_errno(LOG_ERROR, "epoll_wait");
            break;
        }
        if (n > 0)
            last_activity = mono_now();

        for (int i = 0; i < n; i++) {
            uint32_t slot = evs[i].data.u32;

            if (slot == EV_WAKE) {
                /* Drain the eventfd counter, then adopt every queued socket. */
                uint64_t ignored;
                while (read(ctx.reg->wake_fd, &ignored, sizeof ignored) ==
                       (ssize_t)sizeof ignored)
                    ;
                handoff_t items[MAX_CONNS_PER_SESSION];
                int k = owner_drain(ctx.reg, items);
                for (int j = 0; j < k; j++)
                    ec_adopt(&ctx, items[j].fd, items[j].conn_index);
                continue;
            }

            if (slot >= MAX_CONNS_PER_SESSION)
                continue;
            econn_t *c = &ctx.conns[slot];
            if (c->fd < 0)
                continue;

            if (evs[i].events & (EPOLLHUP | EPOLLERR)) {
                ec_close(&ctx, c, "hangup or error", 1);
                continue;
            }
            if (evs[i].events & EPOLLIN) {
                if (ec_on_readable(&ctx, c) < 0)
                    continue;            /* the connection is gone */
            }
            if (c->fd >= 0 && (evs[i].events & EPOLLOUT)) {
                if (ec_pump(&ctx, c) < 0)
                    continue;
            }
        }

        /* Give every live connection a chance to make progress, and refresh
         * what each one is waiting for. */
        for (int i = 0; i < MAX_CONNS_PER_SESSION; i++) {
            econn_t *c = &ctx.conns[i];
            if (c->fd < 0)
                continue;
            if (c->out_sent < c->out_len || c->job != JOB_NONE) {
                if (ec_pump(&ctx, c) < 0)
                    continue;
            }
            if (c->fd >= 0) {
                int want_out = (c->out_sent < c->out_len) || (c->job != JOB_NONE);
                if (want_out != c->want_out)
                    ec_arm(&ctx, c);
            }
        }

        if (ctx.finished)
            break;
        /* With no sockets left the owner waits a while for the client to
         * reconnect its dropped connections, then gives up so an abandoned
         * transfer cannot pin a thread for ever. */
        if (ctx.n_live == 0 && mono_now() - last_activity > (double)g_cfg.idle_sec) {
            log_info("%s: session %016llx owner idle for %us with no "
                     "connections; exiting", ctx.peer,
                     (unsigned long long)ctx.sid, g_cfg.idle_sec);
            break;
        }
    }

out:
    for (int i = 0; i < MAX_CONNS_PER_SESSION; i++)
        if (ctx.conns[i].fd >= 0)
            ec_close(&ctx, &ctx.conns[i], "owner exiting", 0);
    close(ctx.ep);

out_no_ep: {
    double elapsed = mono_now() - ctx.t0;
    uint64_t sent = 0;
    unsigned resumes = 0;
    if (session_snapshot(&g_sessions, ctx.sid, &snap) == 0) {
        sent    = snap.bytes_sent;
        resumes = snap.resumes;
    }
    uint64_t useful    = ctx.tp ? ctx.tp->total_bytes : 0;
    uint64_t redundant = sent > useful ? sent - useful : 0;
    log_info("%s: session %016llx owner finished: %llu payload bytes sent, "
             "%llu redundant, %u fault(s), %u resume(s), %.3f s", ctx.peer,
             (unsigned long long)ctx.sid, (unsigned long long)sent,
             (unsigned long long)redundant, ctx.faults, resumes, elapsed);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "role,case,topic,peer,session,outcome,payload_bytes,"
                   "redundant_bytes,wire_tx_bytes,elapsed_s,fault_checks,faults",
                   "server,parallel,%s,%s,%016llx,%s,%llu,%llu,"
                   "%llu,%.6f,%llu,%llu",
                   ctx.tp ? ctx.tp->key : "?", ctx.peer,
                   (unsigned long long)ctx.sid,
                   ctx.finished ? "complete" : "ended",
                   (unsigned long long)sent, (unsigned long long)redundant,
                   (unsigned long long)ctx.wire_tx, elapsed,
                   (unsigned long long)ctx.fault.checks,
                   (unsigned long long)ctx.fault.fires);

    pthread_mutex_lock(&g_tally_lock);
    g_total_faults += ctx.fault.fires;
    pthread_mutex_unlock(&g_tally_lock);

    session_destroy(&g_sessions, ctx.sid);
    owner_retire(ctx.reg);
    free(start);
    worker_leave();
    return NULL;
}
}

/* ============================ the listener ============================= */
/*
 * The listener accepts a socket, completes its handshake, and then either
 * spawns the session's owner thread (first socket of a session) or hands the
 * socket to the owner that already exists (every subsequent socket).
 */

/* Spawn an owner for `sid`, adopting `fd` as connection `ci`.  Returns 0 on
 * success.  Must be called with g_owners_lock NOT held. */
static int spawn_owner(uint64_t sid, int fd, uint32_t ci, const char *peer)
{
    owner_t *o = calloc(1, sizeof *o);
    owner_start_t *st = calloc(1, sizeof *st);
    if (!o || !st) {
        log_error("out of memory starting a session owner");
        free(o);
        free(st);
        return -1;
    }
    o->sid     = sid;
    o->live    = 1;
    o->wake_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (o->wake_fd < 0) {
        log_errno(LOG_ERROR, "eventfd");
        free(o);
        free(st);
        return -1;
    }
    st->sid              = sid;
    st->reg              = o;
    st->first_fd         = fd;
    st->first_conn_index = ci;
    snprintf(st->peer, sizeof st->peer, "%s", peer);

    pthread_mutex_lock(&g_owners_lock);
    o->next  = g_owners;
    g_owners = o;
    pthread_mutex_unlock(&g_owners_lock);

    pthread_t tid;
    int err = pthread_create(&tid, NULL, owner_main, st);
    if (err != 0) {
        log_error("pthread_create for a session owner: %s", strerror(err));
        owner_retire(o);
        free(st);
        return -1;
    }
    pthread_detach(tid);
    return 0;
}

/* Give `fd` to the session's owner, creating that owner if there is not one.
 * Takes ownership of `fd` in every case. */
static void route_to_owner(uint64_t sid, int fd, uint32_t ci, const char *peer)
{
    pthread_mutex_lock(&g_owners_lock);
    owner_t *o = owner_find_locked(sid);
    if (o) {
        handoff_t item = { .fd = fd, .conn_index = ci };
        int rc = owner_push(o, &item);
        pthread_mutex_unlock(&g_owners_lock);
        if (rc < 0) {
            log_warn("%s: hand-off queue for session %016llx is full; refusing "
                     "conn %u", peer, (unsigned long long)sid, ci);
            send_resp_error(fd, ERR_INTERNAL, "too many connections");
            close(fd);
        } else {
            log_debug("%s: conn %u handed to the existing owner of %016llx",
                      peer, ci, (unsigned long long)sid);
        }
        return;
    }
    pthread_mutex_unlock(&g_owners_lock);

    /* No owner: this socket starts one.  This is the only place a thread is
     * created, and it happens once per client session, never per connection. */
    if (spawn_owner(sid, fd, ci, peer) < 0) {
        send_resp_error(fd, ERR_INTERNAL, "cannot start a session worker");
        close(fd);
    }
}

/* Complete the handshake on a freshly accepted socket, then route it.
 * The socket is still blocking here, with both timeouts set, so a silent
 * client cannot hold the accept loop for longer than the handshake timeout. */
static void handshake_and_route(int fd)
{
    char peer[72];
    net_peer_str(fd, peer, sizeof peer);

    net_set_rcvtimeo(fd, DEFAULT_HANDSHAKE_TIMEO_MS);

    uint64_t wire = 0;
    msg_hdr_t h;
    if (msg_recv_hdr(fd, &h, &wire) != IO_OK) {
        log_info("%s: no usable first message", peer);
        close(fd);
        return;
    }
    if (h.payload_len > FIRST_MSG_CAP) {
        log_warn("%s: first message declares a %u byte payload", peer, h.payload_len);
        send_resp_error(fd, ERR_BAD_REQUEST, "first message too large");
        close(fd);
        return;
    }
    uint8_t pay[FIRST_MSG_CAP];
    if (msg_recv_payload(fd, pay, h.payload_len, &wire) != IO_OK) {
        log_info("%s: truncated first message", peer);
        close(fd);
        return;
    }

    switch (h.type) {
    case MSG_REQ_SESSION_START: {
        char topic[MAX_KEY_LEN];
        if (parse_topic_name(pay, h.payload_len, topic, sizeof topic) < 0 ||
            name_safe_topic(topic) < 0) {
            send_resp_error(fd, ERR_INVALID_NAME, "invalid topic name");
            close(fd);
            return;
        }
        const topic_t *tp = topic_table_lookup(g_topics, topic);
        if (!tp) {
            log_info("%s: unknown topic '%s'", peer, topic);
            send_resp_error(fd, ERR_UNKNOWN_TOPIC, "unknown topic: %s", topic);
            close(fd);
            return;
        }
        uint64_t sid = session_create(&g_sessions, topic, (uint32_t)tp->n_files);
        if (sid == 0) {
            send_resp_error(fd, ERR_INTERNAL, "cannot allocate a session");
            close(fd);
            return;
        }
        if (send_resp_session_id(fd, sid, (uint32_t)tp->n_files, &wire) != IO_OK) {
            session_destroy(&g_sessions, sid);
            close(fd);
            return;
        }
        log_info("%s: new session %016llx for '%s' (%zu files, %.2f MB), K-way "
                 "parallel", peer, (unsigned long long)sid, topic, tp->n_files,
                 (double)tp->total_bytes / 1048576.0);
        route_to_owner(sid, fd, 0, peer);
        return;
    }

    case MSG_REQ_SESSION_RESUME: {
        /* In the Enhanced case this message only re-establishes the session on
         * connection 0 after a client restart.  The real resume is per work
         * item, carried by each connection's own REQ_RANGE and resolved by
         * session_range_begin(), so the position echoed here is advisory. */
        uint64_t sid; uint32_t idx; uint64_t off;
        if (h.payload_len != SZ_REQ_RESUME ||
            parse_sid_idx_off(pay, h.payload_len, &sid, &idx, &off) < 0) {
            send_resp_error(fd, ERR_BAD_REQUEST, "malformed REQ_SESSION_RESUME");
            close(fd);
            return;
        }
        session_t snap;
        if (session_snapshot(&g_sessions, sid, &snap) < 0) {
            log_info("%s: resume refused, session %016llx is unknown or expired",
                     peer, (unsigned long long)sid);
            send_resp_resume_ack(fd, sid, RESUME_REJECTED, 0, &wire);
            close(fd);
            return;
        }
        session_note_resume(&g_sessions, sid);
        if (send_resp_resume_ack(fd, sid, idx, off, &wire) != IO_OK) {
            close(fd);
            return;
        }
        log_info("%s: resuming session %016llx ('%s')", peer,
                 (unsigned long long)sid, snap.topic);
        route_to_owner(sid, fd, 0, peer);
        return;
    }

    case MSG_REQ_ATTACH: {
        uint64_t sid; uint32_t ci;
        if (h.payload_len != SZ_REQ_ATTACH ||
            parse_sid_conn(pay, h.payload_len, &sid, &ci) < 0) {
            send_resp_error(fd, ERR_BAD_REQUEST, "malformed REQ_ATTACH");
            close(fd);
            return;
        }
        session_t snap;
        if (session_snapshot(&g_sessions, sid, &snap) < 0) {
            log_info("%s: attach refused, session %016llx is unknown or expired",
                     peer, (unsigned long long)sid);
            send_resp_error(fd, ERR_NO_SESSION, "no such session");
            close(fd);
            return;
        }
        if (ci == 0 || ci >= MAX_CONNS_PER_SESSION) {
            /* Index 0 belongs to the connection that created the session. */
            send_resp_error(fd, ERR_BAD_REQUEST, "invalid connection index");
            close(fd);
            return;
        }
        if (send_resp_attach_ack(fd, sid, ci, &wire) != IO_OK) {
            close(fd);
            return;
        }
        route_to_owner(sid, fd, ci, peer);
        return;
    }

    default:
        log_warn("%s: first message was %s, which cannot start or join a session",
                 peer, msg_type_name(h.type));
        send_resp_error(fd, ERR_BAD_REQUEST,
                        "expected REQ_SESSION_START, REQ_SESSION_RESUME or REQ_ATTACH");
        close(fd);
        return;
    }
}

/* Housekeeping thread, not a per-client thread. */
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
        pthread_mutex_lock(&g_tally_lock);
        g_total_conns++;
        pthread_mutex_unlock(&g_tally_lock);
        handshake_and_route(cfd);
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
        "  failure_probability  p in [0,1]: chance of dropping a connection\n"
        "                       after each data chunk (0 disables injection)\n"
        "\n"
        "One server thread serves each client, however many parallel\n"
        "connections that client opens.\n"
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
        { "csv",     OPT_STR,  &g_cfg.csv_path,   "append a per-session row to this CSV" },
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
    log_info("Part2 Case 2 Enhanced server listening on port %u: p=%g per "
             "%u-byte chunk, one thread per client multiplexing up to %d "
             "connections each", port, g_failure_prob, g_cfg.chunk_size,
             MAX_CONNS_PER_SESSION);
    log_info("whole-dataset scale: %llu chunks, so a full pass over every topic "
             "would expect %.2f faults at this p",
             (unsigned long long)chunks, fault_expected(g_failure_prob, chunks));

    int rc = accept_loop(lfd);

    log_info("shutting down: no longer accepting connections");
    close(lfd);
    int left = wait_for_workers(5);
    if (left > 0)
        log_warn("%d owner(s) still running after the 5 s grace period; exiting anyway", left);
    else
        log_info("all session owners finished");
    if (have_reaper)
        pthread_join(reaper, NULL);

    size_t live = 0, created = 0, evicted = 0;
    session_table_stats(&g_sessions, &live, &created, &evicted);
    log_info("session summary: %zu created, %zu evicted as idle, %zu still live "
             "at exit", created, evicted, live);
    log_info("connection summary: %llu socket(s) accepted, served by %llu owner "
             "thread(s); %llu fault(s) injected",
             (unsigned long long)g_total_conns, (unsigned long long)g_total_owners,
             (unsigned long long)g_total_faults);

    session_table_destroy(&g_sessions);
    topic_table_free(&table);
    log_close();
    return rc == 0 ? 0 : 1;
}
