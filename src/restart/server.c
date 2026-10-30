/*
 * restart/server.c - multi-threaded file server with fault injection,
 *                        and no session management.
 *
 * Usage:  ./server <port> <topics_root_dir> <failure_probability> [options]
 *
 * HOW THIS WORKS
 * --------------
 * This is the Part I multi-threaded server plus one addition: after every data
 * chunk it writes, the worker asks fault_should_trigger() whether to abandon
 * the connection.  If it says yes, the worker closes the socket without sending
 * RESP_DONE and exits.  Nothing is remembered about the abandoned transfer -
 * that is the whole definition of Case 1 - so when the client reconnects, the
 * server treats it as a brand new request and streams the entire topic again
 * from RESP_META onwards.
 *
 * One thread serves one client, exactly as Part II requires.  The topic table
 * is still built before the listening socket opens and never mutated, so worker
 * threads read it without locking.
 *
 * WHAT THE PROBABILITY MEANS
 * --------------------------
 * p is per emitted 16 KiB chunk, not per transfer: see common/fault_inject.h
 * and README.md.  p = 0 disables injection entirely, which makes this binary a
 * fault-free control that is directly comparable with Part I.
 */
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

static struct {
    unsigned    chunk_size;
    int         nodelay;
    int         sndbuf;
    unsigned    seed;
    const char *log_path;
    const char *csv_path;
    int         quiet;
    int         verbose;
} g_cfg = {
    .chunk_size = DEFAULT_CHUNK_SIZE,
    .nodelay    = 1,
    .sndbuf     = 0,
    .seed       = 0,       /* 0 = derive from the clock and pid */
    .log_path   = NULL,
    .csv_path   = NULL,
    .quiet      = 0,
    .verbose    = 0,
};

static const topic_table_t *g_topics;
static double               g_failure_prob;

/* Process-wide fault tally, for the shutdown summary. */
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
    sa.sa_flags = 0;        /* no SA_RESTART: accept() must return EINTR */
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    /* Dropping connections on purpose makes writes to a dead socket routine,
     * so SIGPIPE must never reach the process. */
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

/* ------------------------- per-connection context ---------------------- */

typedef struct {
    int         fd;
    char        peer[72];
    fault_rng_t fault;      /* this worker's own RNG; never shared */
    uint64_t    wire_tx;
    uint64_t    wire_rx;
    uint64_t    payload_tx;
    uint32_t    files_sent;
} client_ctx_t;

/* How send_one_file() ended. */
typedef enum {
    FILE_OK = 0,      /* the whole file went out                     */
    FILE_FAULT = 1,   /* fault injected: abandon without RESP_DONE   */
    FILE_ERROR = -1   /* socket or filesystem failure                */
} file_rc;

/* ------------------------------ the transfer --------------------------- */

static file_rc send_one_file(client_ctx_t *c, const topic_t *tp, size_t idx,
                             unsigned char *buf)
{
    const file_entry_t *fe = &tp->files[idx];

    char path[MAX_PATH_LEN];
    if (topic_file_path(path, sizeof path, tp, idx) < 0) {
        log_error("%s: path for file %zu does not fit", tp->key, idx);
        send_resp_error(c->fd, ERR_IO, "internal path error");
        return FILE_ERROR;
    }

    int ffd = open(path, O_RDONLY);
    if (ffd < 0) {
        log_warn("open %s: %s", path, strerror(errno));
        send_resp_error(c->fd, ERR_IO, "cannot open %s", fe->name);
        return FILE_ERROR;
    }

    if (send_resp_file_info(c->fd, FLAG_NONE, fe->name, fe->size, fe->crc32,
                            &c->wire_tx) != IO_OK) {
        close(ffd);
        return FILE_ERROR;
    }

    uint64_t sent = 0;
    while (sent < fe->size) {
        size_t want = fe->size - sent < g_cfg.chunk_size
                    ? (size_t)(fe->size - sent) : g_cfg.chunk_size;
        ssize_t n = read(ffd, buf, want);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            log_warn("read %s: %s", path, strerror(errno));
            close(ffd);
            send_resp_error(c->fd, ERR_IO, "read error on %s", fe->name);
            return FILE_ERROR;
        }
        if (n == 0) {
            log_warn("%s: shorter than the cached size", path);
            close(ffd);
            send_resp_error(c->fd, ERR_IO, "file changed on disk: %s", fe->name);
            return FILE_ERROR;
        }
        if (msg_send(c->fd, MSG_RESP_FILE_CHUNK, FLAG_NONE, buf, (uint32_t)n,
                     &c->wire_tx) != IO_OK) {
            close(ffd);
            return FILE_ERROR;
        }
        sent          += (uint64_t)n;
        c->payload_tx += (uint64_t)n;

        /* THE FAULT INJECTION POINT.
         * Evaluated after a chunk has gone out and before the next one, so a
         * fault lands at an arbitrary byte offset inside an arbitrary file.
         * In Case 1 nothing is recorded about how far we got: the client will
         * have to start the whole topic again. */
        if (fault_should_trigger(&c->fault)) {
            log_warn("FAULT injected: %s dropped during %s file %zu (%s) "
                     "at offset %llu/%llu", c->peer, tp->key, idx, fe->name,
                     (unsigned long long)sent, (unsigned long long)fe->size);
            close(ffd);
            return FILE_FAULT;
        }
    }

    close(ffd);
    c->files_sent++;
    return FILE_OK;
}

/* Serve one connection from REQ_TOPIC to RESP_DONE, or until a fault.
 * Returns 1 if a fault was injected, 0 otherwise. */
static int serve_client(client_ctx_t *c)
{
    double t0 = mono_now();

    msg_hdr_t h;
    io_rc rc = msg_recv_hdr(c->fd, &h, &c->wire_rx);
    if (rc != IO_OK) {
        if (rc == IO_EOF)
            log_info("%s: closed before sending a request", c->peer);
        else
            log_warn("%s: error reading request header: %s", c->peer, strerror(errno));
        return 0;
    }
    if (h.type != MSG_REQ_TOPIC) {
        log_warn("%s: expected REQ_TOPIC, got %s", c->peer, msg_type_name(h.type));
        msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "expected REQ_TOPIC");
        return 0;
    }

    uint8_t pay[2 + MAX_KEY_LEN];
    if (h.payload_len > sizeof pay) {
        msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "topic name too long");
        return 0;
    }
    if (msg_recv_payload(c->fd, pay, h.payload_len, &c->wire_rx) != IO_OK) {
        log_warn("%s: truncated REQ_TOPIC", c->peer);
        return 0;
    }

    char topic[MAX_KEY_LEN];
    if (parse_topic_name(pay, h.payload_len, topic, sizeof topic) < 0) {
        send_resp_error(c->fd, ERR_BAD_REQUEST, "malformed REQ_TOPIC");
        return 0;
    }
    if (name_safe_topic(topic) < 0) {
        log_warn("%s: unsafe topic name rejected", c->peer);
        send_resp_error(c->fd, ERR_INVALID_NAME, "invalid topic name");
        return 0;
    }
    const topic_t *tp = topic_table_lookup(g_topics, topic);
    if (!tp) {
        log_info("%s: unknown topic '%s'", c->peer, topic);
        send_resp_error(c->fd, ERR_UNKNOWN_TOPIC, "unknown topic: %s", topic);
        return 0;
    }

    log_info("%s: GET %s (%zu files, %.2f MB) - a fresh transfer, Case 1 keeps "
             "no state", c->peer, topic, tp->n_files,
             (double)tp->total_bytes / 1048576.0);

    if (send_resp_meta(c->fd, (uint32_t)tp->n_files, &c->wire_tx) != IO_OK)
        return 0;

    unsigned char *buf = malloc(g_cfg.chunk_size);
    if (!buf) {
        log_error("out of memory allocating a %u byte chunk buffer", g_cfg.chunk_size);
        send_resp_error(c->fd, ERR_INTERNAL, "server out of memory");
        return 0;
    }

    int faulted = 0, failed = 0;
    for (size_t i = 0; i < tp->n_files; i++) {
        file_rc r = send_one_file(c, tp, i, buf);
        if (r == FILE_FAULT) { faulted = 1; break; }
        if (r == FILE_ERROR) { failed  = 1; break; }
    }
    free(buf);

    double elapsed = mono_now() - t0;

    if (faulted) {
        /* No RESP_DONE and no state kept: the connection just ends. */
        log_info("%s: abandoned %s after %u complete files, %llu payload bytes,"
                 " %.3f s", c->peer, topic, c->files_sent,
                 (unsigned long long)c->payload_tx, elapsed);
    } else if (failed) {
        log_warn("%s: transfer of %s aborted by an error", c->peer, topic);
    } else if (send_resp_done(c->fd, &c->wire_tx) == IO_OK) {
        double mbps = elapsed > 0.0 ? (double)c->payload_tx * 8.0 / elapsed / 1e6 : 0.0;
        log_info("%s: sent %s complete: %u files, %llu payload bytes, %.3f s, "
                 "%.1f Mbit/s", c->peer, topic, c->files_sent,
                 (unsigned long long)c->payload_tx, elapsed, mbps);
    }

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "role,case,topic,peer,outcome,files,payload_bytes,"
                   "wire_tx_bytes,elapsed_s,fault_checks,faults",
                   "server,restart,%s,%s,%s,%u,%llu,%llu,%.6f,%llu,%llu",
                   topic, c->peer,
                   faulted ? "fault" : (failed ? "error" : "complete"),
                   c->files_sent, (unsigned long long)c->payload_tx,
                   (unsigned long long)c->wire_tx, elapsed,
                   (unsigned long long)c->fault.checks,
                   (unsigned long long)c->fault.fires);

    return faulted;
}

/* --------------------------- accept loop ------------------------------- */

static client_ctx_t *ctx_new(int fd)
{
    client_ctx_t *c = calloc(1, sizeof *c);
    if (!c) {
        log_error("out of memory accepting a connection");
        close(fd);
        return NULL;
    }
    c->fd = fd;
    net_peer_str(fd, c->peer, sizeof c->peer);
    /* Each worker seeds its own generator, so concurrent clients never share
     * RNG state and never fail at correlated offsets. */
    fault_init(&c->fault, g_failure_prob);
    return c;
}

static void finish_conn(client_ctx_t *c, int faulted)
{
    pthread_mutex_lock(&g_tally_lock);
    g_total_conns++;
    g_total_faults += c->fault.fires;
    pthread_mutex_unlock(&g_tally_lock);

    if (faulted) {
        /* An injected fault should look like a real disconnection, so the
         * socket is closed immediately with no orderly protocol ending. */
        shutdown(c->fd, SHUT_RDWR);
    } else {
        shutdown(c->fd, SHUT_WR);
    }
    close(c->fd);
    free(c);
}

static void *worker_main(void *arg)
{
    client_ctx_t *c = arg;
    worker_enter();
    int faulted = serve_client(c);
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

        client_ctx_t *c = ctx_new(cfd);
        if (!c)
            continue;

        /* One thread per client, as Part II requires for every case. */
        pthread_t tid;
        int err = pthread_create(&tid, NULL, worker_main, c);
        if (err != 0) {
            log_error("pthread_create: %s; serving inline", strerror(err));
            worker_enter();
            int f = serve_client(c);
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

    int lfd = net_listen(port, DEFAULT_LISTEN_BACKLOG);
    if (lfd < 0) {
        topic_table_free(&table);
        return 1;
    }

    /* Report what p means for this dataset, so a run's log is self-explanatory
     * and nobody mistakes p for "probability per transfer". */
    uint64_t chunks = (g_topics->total_bytes + g_cfg.chunk_size - 1) / g_cfg.chunk_size;
    log_info("Part2 Case 1 server listening on port %u: p=%g per %u-byte chunk, "
             "no session management", port, g_failure_prob, g_cfg.chunk_size);
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
    log_info("fault summary: %llu connection(s) served, %llu fault(s) injected",
             (unsigned long long)g_total_conns, (unsigned long long)g_total_faults);

    topic_table_free(&table);
    log_close();
    return rc == 0 ? 0 : 1;
}
