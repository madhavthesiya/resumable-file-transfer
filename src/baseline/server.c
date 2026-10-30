/*
 * baseline/server.c - topic-based file distribution server, single and
 *                  multi-threaded.
 *
 * Usage:  ./server <port> <topics_root_dir> [single|multi] [options]
 *         (mode defaults to multi)
 *
 * HOW THIS WORKS
 * --------------
 * main() builds the topic table from topics_root_dir (every immediate
 * sub-directory is a topic), opens a listening socket, and then runs one of two
 * accept loops:
 *
 *   single  accept() a connection, serve it to completion on this same thread,
 *           and only then accept() the next one.  A second client waits in the
 *           kernel's listen backlog.  This is the baseline for the comparison.
 *
 *   multi   accept() a connection, hand it to a fresh detached worker thread,
 *           and immediately go back to accept().  One thread serves one client.
 *
 * Both modes call exactly the same serve_topic() function, so any throughput
 * difference between them comes from the concurrency model and nothing else -
 * which is the whole point of the Part I measurement.
 *
 * The topic table is finished before the listening socket exists and is never
 * written to afterwards, so worker threads read it without any lock.
 */
#include "../common/cliargs.h"
#include "../common/common.h"
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

/* ----------------------------- configuration --------------------------- */

static struct {
    unsigned    chunk_size;
    int         nodelay;
    int         sndbuf;
    const char *log_path;
    const char *csv_path;
    int         quiet;
    int         verbose;
} g_cfg = {
    .chunk_size = DEFAULT_CHUNK_SIZE,
    .nodelay    = 1,      /* on by default; evaluated in the report      */
    .sndbuf     = 0,      /* 0 = leave the kernel default alone          */
    .log_path   = NULL,
    .csv_path   = NULL,
    .quiet      = 0,
    .verbose    = 0,
};

static const topic_table_t *g_topics;

/* --------------------------- graceful shutdown ------------------------- */

/* Set from a signal handler, so it must be sig_atomic_t and volatile. */
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
static int             g_workers;      /* live worker threads */

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

/* Install handlers deliberately WITHOUT SA_RESTART: a blocked accept() must
 * return EINTR when the signal arrives, otherwise Ctrl+C would not be noticed
 * until the next client happened to connect. */
static void install_signal_handlers(void)
{
    struct sigaction sa;
    memset(&sa, 0, sizeof sa);
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT,  &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    /* Ignore SIGPIPE process-wide.  Every send() also passes MSG_NOSIGNAL, but
     * ignoring the signal as well means no code path can be killed by a client
     * that vanished mid-transfer. */
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

/* Wait up to `grace_sec` for in-flight workers to finish.  Returns the number
 * still running when the wait gave up (0 means everybody finished). */
static int wait_for_workers(int grace_sec)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += grace_sec;

    pthread_mutex_lock(&g_workers_lock);
    while (g_workers > 0) {
        if (pthread_cond_timedwait(&g_workers_cv, &g_workers_lock, &deadline)
            == ETIMEDOUT)
            break;
    }
    int left = g_workers;
    pthread_mutex_unlock(&g_workers_lock);
    return left;
}

/* ------------------------- per-connection context ---------------------- */

typedef struct {
    int      fd;
    char     peer[72];
    uint64_t wire_tx;      /* every byte written to the socket   */
    uint64_t wire_rx;      /* every byte read from the socket    */
    uint64_t payload_tx;   /* file data bytes only (useful data) */
    uint32_t files_sent;
} client_ctx_t;

/* ------------------------------ the transfer --------------------------- */

/* Stream one file as a RESP_FILE_INFO followed by its data chunks.
 * Returns 0 on success, -1 if the connection or the file failed. */
static int send_one_file(client_ctx_t *c, const topic_t *tp, size_t idx,
                         unsigned char *buf)
{
    const file_entry_t *fe = &tp->files[idx];

    char path[MAX_PATH_LEN];
    if (topic_file_path(path, sizeof path, tp, idx) < 0) {
        log_error("%s: path for file %zu does not fit", tp->key, idx);
        send_resp_error(c->fd, ERR_IO, "internal path error");
        return -1;
    }

    int ffd = open(path, O_RDONLY);
    if (ffd < 0) {
        /* A file that disappeared between start-up and now: report it to the
         * client rather than silently sending a short file. */
        log_warn("open %s: %s", path, strerror(errno));
        send_resp_error(c->fd, ERR_IO, "cannot open %s", fe->name);
        return -1;
    }

    if (send_resp_file_info(c->fd, FLAG_NONE, fe->name, fe->size, fe->crc32,
                            &c->wire_tx) != IO_OK) {
        close(ffd);
        return -1;
    }

    /* A zero-length file is just its RESP_FILE_INFO: no chunks at all. */
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
            return -1;
        }
        if (n == 0) {
            /* The file shrank since start-up; the size we announced is now a
             * lie, so fail this transfer loudly instead of hanging the client. */
            log_warn("%s: shorter than the cached size", path);
            close(ffd);
            send_resp_error(c->fd, ERR_IO, "file changed on disk: %s", fe->name);
            return -1;
        }
        if (msg_send(c->fd, MSG_RESP_FILE_CHUNK, FLAG_NONE, buf, (uint32_t)n,
                     &c->wire_tx) != IO_OK) {
            close(ffd);
            return -1;
        }
        sent          += (uint64_t)n;
        c->payload_tx += (uint64_t)n;
    }

    close(ffd);
    c->files_sent++;
    return 0;
}

/* Serve one client from its REQ_TOPIC to RESP_DONE.  This is the function both
 * accept loops call, so single- and multi-threaded modes transfer bytes through
 * identical code. */
static void serve_client(client_ctx_t *c)
{
    double t0 = mono_now();

    /* ---- 1. read the request ---- */
    msg_hdr_t h;
    io_rc rc = msg_recv_hdr(c->fd, &h, &c->wire_rx);
    if (rc != IO_OK) {
        if (rc == IO_EOF)
            log_info("%s: closed before sending a request", c->peer);
        else
            log_warn("%s: error reading request header: %s", c->peer, strerror(errno));
        return;
    }
    if (h.type != MSG_REQ_TOPIC) {
        log_warn("%s: expected REQ_TOPIC, got %s", c->peer, msg_type_name(h.type));
        msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "expected REQ_TOPIC");
        return;
    }

    uint8_t pay[2 + MAX_KEY_LEN];
    if (h.payload_len > sizeof pay) {
        log_warn("%s: REQ_TOPIC payload too large (%u)", c->peer, h.payload_len);
        msg_skip_payload(c->fd, h.payload_len, &c->wire_rx);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "topic name too long");
        return;
    }
    if (msg_recv_payload(c->fd, pay, h.payload_len, &c->wire_rx) != IO_OK) {
        log_warn("%s: truncated REQ_TOPIC", c->peer);
        return;
    }

    char topic[MAX_KEY_LEN];
    if (parse_topic_name(pay, h.payload_len, topic, sizeof topic) < 0) {
        log_warn("%s: malformed REQ_TOPIC", c->peer);
        send_resp_error(c->fd, ERR_BAD_REQUEST, "malformed REQ_TOPIC");
        return;
    }

    /* ---- 2. validate and look the topic up ---- */
    /* Name safety first: a name containing ".." or '/' is refused before it is
     * used for anything, so a malformed request can never reach outside
     * topics_root_dir. */
    if (name_safe_topic(topic) < 0) {
        log_warn("%s: unsafe topic name rejected", c->peer);
        send_resp_error(c->fd, ERR_INVALID_NAME, "invalid topic name");
        return;
    }
    const topic_t *tp = topic_table_lookup(g_topics, topic);
    if (!tp) {
        log_info("%s: unknown topic '%s'", c->peer, topic);
        send_resp_error(c->fd, ERR_UNKNOWN_TOPIC, "unknown topic: %s", topic);
        return;
    }
    log_info("%s: GET %s (%zu files, %.2f MB)", c->peer, topic, tp->n_files,
             (double)tp->total_bytes / 1048576.0);

    /* ---- 3. announce and stream ---- */
    if (send_resp_meta(c->fd, (uint32_t)tp->n_files, &c->wire_tx) != IO_OK) {
        log_warn("%s: failed to send RESP_META", c->peer);
        return;
    }

    unsigned char *buf = malloc(g_cfg.chunk_size);
    if (!buf) {
        log_error("out of memory allocating a %u byte chunk buffer", g_cfg.chunk_size);
        send_resp_error(c->fd, ERR_INTERNAL, "server out of memory");
        return;
    }

    int ok = 1;
    for (size_t i = 0; i < tp->n_files; i++) {
        if (send_one_file(c, tp, i, buf) < 0) { ok = 0; break; }
    }
    free(buf);

    if (!ok) {
        log_warn("%s: transfer of '%s' aborted after %u files",
                 c->peer, topic, c->files_sent);
        return;
    }

    if (send_resp_done(c->fd, &c->wire_tx) != IO_OK) {
        log_warn("%s: failed to send RESP_DONE", c->peer);
        return;
    }

    /* ---- 4. record the measurement ---- */
    double elapsed = mono_now() - t0;
    double mbps = elapsed > 0.0 ? (double)c->payload_tx * 8.0 / elapsed / 1e6 : 0.0;
    log_info("%s: sent %s complete: %u files, %llu payload bytes, %llu wire bytes,"
             " %.3f s, %.1f Mbit/s", c->peer, topic, c->files_sent,
             (unsigned long long)c->payload_tx, (unsigned long long)c->wire_tx,
             elapsed, mbps);

    if (g_cfg.csv_path)
        csv_append(g_cfg.csv_path,
                   "role,topic,peer,files,payload_bytes,wire_tx_bytes,"
                   "wire_rx_bytes,elapsed_s,throughput_mbps",
                   "server,%s,%s,%u,%llu,%llu,%llu,%.6f,%.3f",
                   topic, c->peer, c->files_sent,
                   (unsigned long long)c->payload_tx,
                   (unsigned long long)c->wire_tx,
                   (unsigned long long)c->wire_rx, elapsed, mbps);
}

/* --------------------------- accept loops ------------------------------ */

/* Apply the socket-level options to a freshly accepted connection. */
static void tune_conn(int fd)
{
    if (g_cfg.nodelay)
        net_set_nodelay(fd, 1);
    if (g_cfg.sndbuf > 0)
        net_set_sndbuf(fd, g_cfg.sndbuf);
}

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
    return c;
}

/* Multi-threaded mode: one detached thread per client. */
static void *worker_main(void *arg)
{
    client_ctx_t *c = arg;
    worker_enter();
    serve_client(c);
    /* shutdown() before close() so the client sees an orderly FIN even if it
     * has not finished reading; close() alone can discard queued data on some
     * configurations. */
    shutdown(c->fd, SHUT_WR);
    close(c->fd);
    free(c);
    worker_leave();
    return NULL;
}

/* Returns 0 on a normal (signalled) exit, -1 on a fatal accept error. */
static int accept_loop(int lfd, int multi)
{
    while (!stop_requested()) {
        int cfd = accept(lfd, NULL, NULL);
        if (cfd < 0) {
            if (errno == EINTR)
                continue;          /* a signal arrived; re-check g_stop */
            if (errno == ECONNABORTED || errno == EMFILE || errno == ENFILE) {
                /* Transient: log and keep serving rather than dying. */
                log_errno(LOG_WARN, "accept");
                continue;
            }
            log_errno(LOG_ERROR, "accept");
            return -1;
        }
        tune_conn(cfd);

        client_ctx_t *c = ctx_new(cfd);
        if (!c)
            continue;

        if (!multi) {
            /* Single-threaded: serve to completion here.  The next client sits
             * in the listen backlog until this returns. */
            worker_enter();
            serve_client(c);
            shutdown(c->fd, SHUT_WR);
            close(c->fd);
            free(c);
            worker_leave();
            continue;
        }

        pthread_t tid;
        int err = pthread_create(&tid, NULL, worker_main, c);
        if (err != 0) {
            /* Out of threads: serve this one inline rather than dropping it. */
            log_error("pthread_create: %s; serving inline", strerror(err));
            worker_enter();
            serve_client(c);
            shutdown(c->fd, SHUT_WR);
            close(c->fd);
            free(c);
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
        "usage: %s <port> <topics_root_dir> [single|multi] [options]\n"
        "\n"
        "  port              TCP port to listen on\n"
        "  topics_root_dir   directory whose sub-directories are the topics\n"
        "  single|multi      threading mode (optional, default: multi)\n"
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
        { "log",     OPT_STR,  &g_cfg.log_path,   "also append logs to this file" },
        { "csv",     OPT_STR,  &g_cfg.csv_path,   "append a per-transfer row to this CSV" },
        { "quiet",   OPT_FLAG, &g_cfg.quiet,      "log warnings and errors only" },
        { "verbose", OPT_FLAG, &g_cfg.verbose,    "log debug detail as well" },
    };
    const size_t nspecs = sizeof specs / sizeof specs[0];

    if (argc < 3) {
        usage(argv[0], specs, nspecs);
        return 2;
    }

    uint16_t port;
    if (parse_port(argv[1], &port) < 0) {
        fprintf(stderr, "error: '%s' is not a valid TCP port (1-65535)\n", argv[1]);
        return 2;
    }
    const char *root = argv[2];

    /* The optional mode argument is positional and bare (not --name value), so
     * that the required invocation `./server <port> <dir>` stays untouched. */
    int multi = 1;
    int first_opt = 3;
    if (argc > 3 && argv[3][0] != '-') {
        if (strcmp(argv[3], "single") == 0)      multi = 0;
        else if (strcmp(argv[3], "multi") == 0)  multi = 1;
        else {
            fprintf(stderr, "error: mode must be 'single' or 'multi', got '%s'\n", argv[3]);
            return 2;
        }
        first_opt = 4;
    }
    if (opts_parse(argc, argv, first_opt, specs, nspecs) < 0)
        return 2;

    if (g_cfg.chunk_size == 0 || g_cfg.chunk_size > MAX_PAYLOAD_LEN) {
        fprintf(stderr, "error: --chunk must be between 1 and %u\n", MAX_PAYLOAD_LEN);
        return 2;
    }

    log_init(g_cfg.log_path,
             g_cfg.verbose ? LOG_DEBUG : (g_cfg.quiet ? LOG_WARN : LOG_INFO));
    install_signal_handlers();

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
    log_info("baseline server listening on port %u in %s-threaded mode "
             "(chunk=%u, nodelay=%d)", port, multi ? "multi" : "single",
             g_cfg.chunk_size, g_cfg.nodelay);

    int rc = accept_loop(lfd, multi);

    /* Graceful shutdown: stop accepting, then give the workers a bounded grace
     * period so a demonstration interrupted with Ctrl+C does not leave clients
     * guessing what happened. */
    log_info("shutting down: no longer accepting connections");
    close(lfd);
    int left = wait_for_workers(5);
    if (left > 0)
        log_warn("%d worker(s) still running after the 5 s grace period; exiting anyway", left);
    else
        log_info("all workers finished");

    topic_table_free(&table);
    log_close();
    return rc == 0 ? 0 : 1;
}
