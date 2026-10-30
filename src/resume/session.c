#include "session.h"

#include "../common/logging.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* Spread ids across buckets.  Session ids are already random, so the low bits
 * are as good as any hash; the multiply keeps this true even if the id source
 * is ever changed to something sequential. */
static size_t bucket_of(uint64_t id)
{
    return (size_t)((id * 1099511628211ull) % SESSION_BUCKETS);
}

int session_table_init(session_table_t *t, unsigned idle_timeout_sec)
{
    memset(t, 0, sizeof *t);
    t->idle_timeout_sec = idle_timeout_sec;
    if (pthread_mutex_init(&t->lock, NULL) != 0) {
        log_error("cannot initialise the session table mutex");
        return -1;
    }
    t->urandom_fd = open("/dev/urandom", O_RDONLY);
    if (t->urandom_fd < 0) {
        log_errno(LOG_ERROR, "open /dev/urandom");
        pthread_mutex_destroy(&t->lock);
        return -1;
    }
    return 0;
}

void session_table_destroy(session_table_t *t)
{
    pthread_mutex_lock(&t->lock);
    for (size_t b = 0; b < SESSION_BUCKETS; b++) {
        session_t *s = t->buckets[b];
        while (s) {
            session_t *next = s->next;
            free(s);
            s = next;
        }
        t->buckets[b] = NULL;
    }
    t->live = 0;
    if (t->urandom_fd >= 0) {
        close(t->urandom_fd);
        t->urandom_fd = -1;
    }
    pthread_mutex_unlock(&t->lock);
    pthread_mutex_destroy(&t->lock);
}

/* Caller must hold the lock. */
static session_t *find_locked(session_table_t *t, uint64_t id)
{
    for (session_t *s = t->buckets[bucket_of(id)]; s; s = s->next)
        if (s->id == id)
            return s;
    return NULL;
}

/* Caller must hold the lock.  Returns 0 on success, -1 if urandom failed. */
static int fresh_id_locked(session_table_t *t, uint64_t *out)
{
    for (int attempt = 0; attempt < 64; attempt++) {
        uint64_t id = 0;
        size_t got = 0;
        while (got < sizeof id) {
            ssize_t n = read(t->urandom_fd, (char *)&id + got, sizeof id - got);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                log_errno(LOG_ERROR, "read /dev/urandom");
                return -1;
            }
            if (n == 0) {
                log_error("unexpected EOF on /dev/urandom");
                return -1;
            }
            got += (size_t)n;
        }
        /* 0 is reserved to mean "no session", and a collision - while
         * essentially impossible for a 64-bit random value - must not be
         * allowed to alias two transfers onto one record. */
        if (id != 0 && !find_locked(t, id)) {
            *out = id;
            return 0;
        }
    }
    log_error("could not generate a unique session id");
    return -1;
}

uint64_t session_create(session_table_t *t, const char *topic, uint32_t file_count)
{
    session_t *s = calloc(1, sizeof *s);
    if (!s) {
        log_error("out of memory creating a session");
        return 0;
    }

    pthread_mutex_lock(&t->lock);
    uint64_t id = 0;
    if (fresh_id_locked(t, &id) < 0) {
        pthread_mutex_unlock(&t->lock);
        free(s);
        return 0;
    }
    s->id            = id;
    snprintf(s->topic, sizeof s->topic, "%s", topic);
    s->file_count    = file_count;
    s->ack_file_idx  = 0;
    s->ack_offset    = 0;
    s->last_activity = time(NULL);

    size_t b = bucket_of(id);
    s->next = t->buckets[b];
    t->buckets[b] = s;
    t->live++;
    t->created++;
    pthread_mutex_unlock(&t->lock);

    return id;
}

int session_snapshot(session_table_t *t, uint64_t id, session_t *out)
{
    pthread_mutex_lock(&t->lock);
    session_t *s = find_locked(t, id);
    if (s) {
        s->last_activity = time(NULL);
        *out = *s;
        out->next = NULL;     /* the copy owns no list linkage */
    }
    pthread_mutex_unlock(&t->lock);
    return s ? 0 : -1;
}

int session_ack(session_table_t *t, uint64_t id, uint32_t file_idx, uint64_t offset)
{
    pthread_mutex_lock(&t->lock);
    session_t *s = find_locked(t, id);
    if (s) {
        /* Monotonic: a late or duplicated ACK can never move the record
         * backwards, which would otherwise cause a needless retransmission. */
        if (pos_before(s->ack_file_idx, s->ack_offset, file_idx, offset)) {
            s->ack_file_idx = file_idx;
            s->ack_offset   = offset;
        }
        s->last_activity = time(NULL);
    }
    pthread_mutex_unlock(&t->lock);
    return s ? 0 : -1;
}

int session_mark_sent(session_table_t *t, uint64_t id, uint32_t file_idx,
                      uint64_t offset, uint64_t bytes)
{
    pthread_mutex_lock(&t->lock);
    session_t *s = find_locked(t, id);
    if (s) {
        s->bytes_sent += bytes;
        /* Monotonic: a resume rewinds what we send next, but the high-water
         * mark of what has ever been sent must not move backwards. */
        if (pos_before(s->sent_file_idx, s->sent_offset, file_idx, offset)) {
            s->sent_file_idx = file_idx;
            s->sent_offset   = offset;
        }
        s->last_activity = time(NULL);
    }
    pthread_mutex_unlock(&t->lock);
    return s ? 0 : -1;
}

/* The three event counters differ only in which field they touch. */
static int note_event(session_table_t *t, uint64_t id, int which)
{
    pthread_mutex_lock(&t->lock);
    session_t *s = find_locked(t, id);
    if (s) {
        if (which == 0)      s->resumes++;
        else if (which == 1) s->rejects++;
        else                 s->faults++;
        s->last_activity = time(NULL);
    }
    pthread_mutex_unlock(&t->lock);
    return s ? 0 : -1;
}

int session_note_resume(session_table_t *t, uint64_t id) { return note_event(t, id, 0); }
int session_note_reject(session_table_t *t, uint64_t id) { return note_event(t, id, 1); }
int session_note_fault (session_table_t *t, uint64_t id) { return note_event(t, id, 2); }

void session_destroy(session_table_t *t, uint64_t id)
{
    pthread_mutex_lock(&t->lock);
    size_t b = bucket_of(id);
    session_t **link = &t->buckets[b];
    while (*link) {
        if ((*link)->id == id) {
            session_t *dead = *link;
            *link = dead->next;
            free(dead);
            t->live--;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&t->lock);
}

size_t session_evict_idle(session_table_t *t)
{
    time_t now = time(NULL);
    size_t removed = 0;

    pthread_mutex_lock(&t->lock);
    for (size_t b = 0; b < SESSION_BUCKETS; b++) {
        session_t **link = &t->buckets[b];
        while (*link) {
            session_t *s = *link;
            if (now - s->last_activity > (time_t)t->idle_timeout_sec) {
                *link = s->next;
                log_info("session %016llx (%s) evicted after %lld s idle",
                         (unsigned long long)s->id, s->topic,
                         (long long)(now - s->last_activity));
                free(s);
                t->live--;
                t->evicted++;
                removed++;
                continue;            /* *link now points at the next entry */
            }
            link = &s->next;
        }
    }
    pthread_mutex_unlock(&t->lock);
    return removed;
}

void session_table_stats(session_table_t *t, size_t *live, size_t *created,
                         size_t *evicted)
{
    pthread_mutex_lock(&t->lock);
    if (live)    *live    = t->live;
    if (created) *created = t->created;
    if (evicted) *evicted = t->evicted;
    pthread_mutex_unlock(&t->lock);
}
