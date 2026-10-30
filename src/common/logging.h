/*
 * logging.h - timestamped, thread-safe, single-line logging.
 *
 * HOW THIS WORKS
 * --------------
 * One mutex serialises writes so two worker threads can never interleave
 * halves of a line.  Every record looks like
 *
 *   [2026-10-22T01:33:07.412] [INFO ] [tid 7f2a] message text
 *
 * Output goes to stderr and, if log_init() was given a path, additionally to
 * that file.  stdout is deliberately left alone: the client prints its
 * machine-readable RESULT summary there, so the benchmark harness can parse
 * stdout without having to filter log noise out of it.
 */
#ifndef IT305_LOGGING_H
#define IT305_LOGGING_H

typedef enum {
    LOG_DEBUG = 0,
    LOG_INFO  = 1,
    LOG_WARN  = 2,
    LOG_ERROR = 3
} log_level_t;

/* `path` may be NULL for stderr only.  Records below `min_level` are dropped.
 * Safe to call once, before any threads are created. */
void log_init(const char *path, log_level_t min_level);

/* Flush and close the log file (if any).  Safe to call without log_init(). */
void log_close(void);

/* Raise or lower the threshold at run time (used by --quiet / --verbose). */
void log_set_level(log_level_t min_level);

void log_msg(log_level_t level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));

#define log_debug(...) log_msg(LOG_DEBUG, __VA_ARGS__)
#define log_info(...)  log_msg(LOG_INFO,  __VA_ARGS__)
#define log_warn(...)  log_msg(LOG_WARN,  __VA_ARGS__)
#define log_error(...) log_msg(LOG_ERROR, __VA_ARGS__)

/* Log `what` together with the current errno and its strerror() text.  Used at
 * every failed syscall site so no failure is ever silently swallowed. */
void log_errno(log_level_t level, const char *what);

#endif /* IT305_LOGGING_H */
