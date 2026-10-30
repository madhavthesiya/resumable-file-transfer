#include "common.h"

#include <errno.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

double mono_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

void sleep_ms(unsigned ms)
{
    struct timespec req, rem;
    req.tv_sec  = (time_t)(ms / 1000u);
    req.tv_nsec = (long)(ms % 1000u) * 1000000L;
    /* nanosleep() returns early with EINTR when a signal arrives; finish the
     * remaining time rather than returning a shorter sleep than asked for. */
    while (nanosleep(&req, &rem) < 0 && errno == EINTR)
        req = rem;
}

int mkdir_p(const char *path, mode_t mode)
{
    char buf[MAX_PATH_LEN];
    size_t n = strlen(path);
    if (n == 0 || n >= sizeof buf) {
        errno = (n == 0) ? EINVAL : ENAMETOOLONG;
        return -1;
    }
    memcpy(buf, path, n + 1);

    /* Walk the path left to right, temporarily terminating at each '/' so the
     * prefix can be created.  An already-existing component is not an error. */
    for (char *p = buf + 1; *p; p++) {
        if (*p != '/')
            continue;
        *p = '\0';
        if (mkdir(buf, mode) < 0 && errno != EEXIST)
            return -1;
        *p = '/';
    }
    if (mkdir(buf, mode) < 0 && errno != EEXIST)
        return -1;
    return 0;
}

int csv_append(const char *path, const char *header, const char *fmt, ...)
{
    int fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0)
        return -1;

    /* flock() makes the "is it empty? then write the header" test and the row
     * append atomic with respect to other processes doing the same thing, which
     * is what lets 8 concurrent clients share one results file. */
    if (flock(fd, LOCK_EX) < 0) {
        close(fd);
        return -1;
    }

    int rc = 0;
    struct stat st;
    if (fstat(fd, &st) == 0 && st.st_size == 0 && header && *header) {
        if (dprintf(fd, "%s\n", header) < 0)
            rc = -1;
    }
    if (rc == 0) {
        va_list ap;
        va_start(ap, fmt);
        if (vdprintf(fd, fmt, ap) < 0)
            rc = -1;
        va_end(ap);
        if (rc == 0 && dprintf(fd, "\n") < 0)
            rc = -1;
    }

    flock(fd, LOCK_UN);
    if (close(fd) < 0)
        rc = -1;
    return rc;
}
