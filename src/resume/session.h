/*
 * session.h - the server-side session table for Part II Case 2.
 *
 * HOW THIS WORKS
 * --------------
 * A session is the server's record of one in-progress topic transfer.  It
 * outlives the TCP connection that created it, which is the whole point: when a
 * fault drops the connection, the client reconnects, quotes its session_id, and
 * the server picks the transfer up from the recorded offset instead of starting
 * over.
 *
 * Unlike the topic table (built once, then read-only and lock-free), this table
 * is mutated constantly - a new entry per transfer, an update per acknowledged
 * checkpoint, a removal on completion - so every operation takes one mutex.
 * The critical sections only touch a few struct fields and never perform file
 * or socket I/O, so they are microseconds long and do not become a bottleneck.
 *
 * Callers never hold a pointer into the table.  session_snapshot() copies the
 * fields a worker needs while the lock is held, and the worker then works from
 * its own copy and writes progress back through session_ack().  That makes it
 * impossible for a worker to read a session that another thread is freeing.
 *
 * WHAT COUNTS AS PROGRESS
 * -----------------------
 * ack_file_idx / ack_offset advance ONLY on a REQ_ACK from the client - never
 * because the server sent something.  This is what makes the resume clamp in
 * PROTOCOL.md section 5.2.1 meaningful: the server resumes from the smaller of
 * "what the client claims" and "what the client has actually confirmed", so a
 * client whose checkpoint file is wrong or stale can never cause the server to
 * skip data the client does not have.
 */
#ifndef IT305_SESSION_H
#define IT305_SESSION_H

#include "../common/common.h"

#include <pthread.h>
#include <time.h>

#define SESSION_BUCKETS 128

typedef struct session {
    uint64_t id;
    char     topic[MAX_KEY_LEN];
    uint32_t file_count;

    /* Client-confirmed progress.  Advanced only by session_ack().  Kept for
     * the metrics and for the report's discussion of ACK lag; it is NOT what
     * the resume clamp uses - see the note on sent_* below. */
    uint32_t ack_file_idx;
    uint64_t ack_offset;

    /* The furthest position this session has actually transmitted.  This is
     * what a resume is clamped against, so the server can never be talked into
     * skipping bytes it never sent, while still honouring progress the client
     * has durably written.  (The acknowledged position cannot serve that role:
     * TCP send buffering lets the server run megabytes ahead of the client's
     * ACK stream, so ack_* lags by the socket buffer rather than by one
     * checkpoint interval.  Clamping to it discarded real progress - see
     * README, "Deviations from the design document".) */
    uint32_t sent_file_idx;
    uint64_t sent_offset;

    /* Metrics, for the log and the CSV. */
    uint64_t bytes_sent;      /* payload bytes this session has emitted,
                               * summed over every connection it used.  The
                               * redundant total is this minus the topic size,
                               * which is exact and needs no extra bookkeeping. */
    unsigned resumes;         /* successful resume handshakes                */
    unsigned rejects;         /* resume requests refused                     */
    unsigned faults;          /* injected faults on this session             */

    time_t   last_activity;
    struct session *next;     /* separate chaining within a bucket           */
} session_t;

typedef struct {
    session_t      *buckets[SESSION_BUCKETS];
    pthread_mutex_t lock;
    int             urandom_fd;   /* source of session ids                   */
    size_t          live;         /* sessions currently in the table         */
    size_t          created;      /* lifetime counter, for the summary       */
    size_t          evicted;
    unsigned        idle_timeout_sec;
} session_table_t;

/* Lexicographic "is position a before position b" over (file_idx, offset).
 * The resume clamp is a minimum under this ordering, which generalises the
 * design document's min(client_offset, bytes_acked) to the case where the two
 * sides disagree about the file as well as the offset. */
static inline int pos_before(uint32_t a_idx, uint64_t a_off,
                             uint32_t b_idx, uint64_t b_off)
{
    return (a_idx < b_idx) || (a_idx == b_idx && a_off < b_off);
}

/* Returns 0 on success, -1 on failure (reason logged). */
int  session_table_init(session_table_t *t, unsigned idle_timeout_sec);
void session_table_destroy(session_table_t *t);

/* Create a session for `topic`.  Returns its id, or 0 on failure.
 * Ids are 8 random bytes from /dev/urandom, retried on the (astronomically
 * unlikely) collision, and never 0 so that 0 can mean "no session". */
uint64_t session_create(session_table_t *t, const char *topic, uint32_t file_count);

/* Copy the session into *out and refresh its idle timer.
 * Returns 0 on success, -1 if there is no such session. */
int session_snapshot(session_table_t *t, uint64_t id, session_t *out);

/* Advance the client-confirmed position, but only ever forwards.
 * Returns 0 on success, -1 if there is no such session. */
int session_ack(session_table_t *t, uint64_t id, uint32_t file_idx, uint64_t offset);

/* Record that the session has now transmitted up to (file_idx, offset) and add
 * `bytes` to its emitted-payload counter.  The position only ever advances. */
int session_mark_sent(session_table_t *t, uint64_t id, uint32_t file_idx,
                      uint64_t offset, uint64_t bytes);

/* Bump one of the event counters.  Returns 0 on success, -1 if absent. */
int session_note_resume(session_table_t *t, uint64_t id);
int session_note_reject(session_table_t *t, uint64_t id);
int session_note_fault (session_table_t *t, uint64_t id);

void   session_destroy(session_table_t *t, uint64_t id);

/* Remove sessions idle for longer than the configured timeout.  Returns how
 * many were removed.  This bounds memory when a client abandons a transfer
 * without ever coming back. */
size_t session_evict_idle(session_table_t *t);

/* Snapshot of the table-wide counters, for the shutdown summary. */
void session_table_stats(session_table_t *t, size_t *live, size_t *created,
                         size_t *evicted);

#endif /* IT305_SESSION_H */
