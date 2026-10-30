#include "common.h"
#include "logging.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

static FILE           *g_file;                    /* optional second sink   */
static log_level_t     g_min = LOG_INFO;
static pthread_mutex_t g_lock = PTHREAD_MUTEX_INITIALIZER;

static const char *level_name(log_level_t l)
{
    switch (l) {
    case LOG_DEBUG: return "DEBUG";
    case LOG_INFO:  return "INFO ";
    case LOG_WARN:  return "WARN ";
    case LOG_ERROR: return "ERROR";
    default:        return "?????";
    }
}

void log_init(const char *path, log_level_t min_level)
{
    g_min = min_level;
    if (path && *path) {
        g_file = fopen(path, "a");
        if (!g_file)
            fprintf(stderr, "warning: cannot open log file %s: %s\n",
                    path, strerror(errno));
    }
}

void log_set_level(log_level_t min_level) { g_min = min_level; }

void log_close(void)
{
    pthread_mutex_lock(&g_lock);
    if (g_file) {
        fclose(g_file);
        g_file = NULL;
    }
    pthread_mutex_unlock(&g_lock);
}

/* Format "2026-10-22T01:33:07.412" into `out`. */
static void stamp(char *out, size_t n)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    struct tm tm;
    localtime_r(&tv.tv_sec, &tm);
    size_t k = strftime(out, n, "%Y-%m-%dT%H:%M:%S", &tm);
    snprintf(out + k, n - k, ".%03ld", (long)(tv.tv_usec / 1000));
}

void log_msg(log_level_t level, const char *fmt, ...)
{
    if (level < g_min)
        return;

    char ts[40];
    stamp(ts, sizeof ts);

    /* Render the caller's text into a bounded buffer first, so the whole record
     * can be emitted with one fputs() per sink while holding the lock. */
    char body[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof body, fmt, ap);
    va_end(ap);

    /* Low 16 bits of the thread id: enough to tell workers apart in a log, far
     * shorter than the full pthread_t. */
    unsigned short tid = (unsigned short)((unsigned long)pthread_self() & 0xFFFFu);

    pthread_mutex_lock(&g_lock);
    fprintf(stderr, "[%s] [%s] [tid %04x] %s\n", ts, level_name(level), tid, body);
    if (g_file) {
        fprintf(g_file, "[%s] [%s] [tid %04x] %s\n", ts, level_name(level), tid, body);
        fflush(g_file);
    }
    pthread_mutex_unlock(&g_lock);
}

void log_errno(log_level_t level, const char *what)
{
    int e = errno;                       /* capture before anything clobbers it */
    log_msg(level, "%s: %s (errno=%d)", what, strerror(e), e);
    errno = e;
}
