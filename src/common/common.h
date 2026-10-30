/*
 * common.h - project-wide limits, tunable defaults and tiny helpers.
 *
 * HOW THIS WORKS
 * --------------
 * Every executable in this project (8 of them: a server and a client for Part 1
 * and for each of the three Part 2 cases) includes this header first.  It fixes
 * the compile-time limits and the default values of every tunable named in
 * PROTOCOL.md section 8.  Nothing here depends on sockets or threads, so it is
 * safe to include from any module.
 */
#ifndef IT305_COMMON_H
#define IT305_COMMON_H

/* scandir/alphasort, MSG_NOSIGNAL, eventfd, accept4.  Guarded because the
 * Makefiles also pass -D_GNU_SOURCE, and redefining it is a warning. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* ---- limits (compile time) ------------------------------------------------ */

#define MAX_TOPICS        256     /* topics (sub-directories) per root       */
#define MAX_KEY_LEN        64     /* topic name, including NUL               */
#define MAX_NAME_LEN      256     /* file name, including NUL                */
#define MAX_PATH_LEN     1024     /* server-side absolute path, incl. NUL    */
#define MAX_PAYLOAD_LEN  (16u * 1024u * 1024u)   /* refuse anything larger   */
#define MAX_CONNS_PER_SESSION 16  /* caps K in the Enhanced case             */

/* ---- tunable defaults (overridable from the command line) ---------------- */

#define DEFAULT_CHUNK_SIZE        16384u    /* bytes of file data per message */
#define DEFAULT_CKPT_INTERVAL     65536u    /* client checkpoint cadence      */
#define DEFAULT_SPLIT_THRESHOLD  524288u    /* split files >= this across K   */
#define DEFAULT_PARALLEL_CONNS        4     /* K, Enhanced case               */
#define DEFAULT_SESSION_IDLE_SEC     60     /* idle-session eviction          */
#define DEFAULT_MAX_RETRIES         500     /* client reconnection attempts   */
#define DEFAULT_BACKOFF_MS_BASE      10     /* 10ms * 2^n ...                 */
#define DEFAULT_BACKOFF_MS_MAX      500     /* ... capped here                */
#define DEFAULT_LISTEN_BACKLOG      128
#define DEFAULT_HANDSHAKE_TIMEO_MS 5000     /* first message on a new socket  */

/* ---- small helpers ------------------------------------------------------- */

/* Minimum/maximum for the 64-bit unsigned quantities used for file offsets.
 * Written as functions rather than macros so arguments are evaluated once. */
static inline uint64_t u64_min(uint64_t a, uint64_t b) { return a < b ? a : b; }
static inline uint64_t u64_max(uint64_t a, uint64_t b) { return a > b ? a : b; }

/* Seconds since an unspecified epoch, from CLOCK_MONOTONIC.  Monotonic time is
 * what every measurement in this project uses: it never jumps backwards when
 * the wall clock is adjusted, so an elapsed-time difference is always sane. */
double mono_now(void);

/* Sleep for the given number of milliseconds, retrying across EINTR. */
void sleep_ms(unsigned ms);

/* Create every missing component of `path`, like `mkdir -p`.
 * Returns 0 on success, -1 with errno set otherwise. */
int mkdir_p(const char *path, mode_t mode);

/* Append one line to a CSV file, creating it and writing `header` first if the
 * file is new or empty.  The whole operation is serialised with flock(), so
 * several concurrently running clients may safely append to the same file.
 * Returns 0 on success, -1 on error. */
int csv_append(const char *path, const char *header, const char *fmt, ...)
    __attribute__((format(printf, 3, 4)));

#endif /* IT305_COMMON_H */
