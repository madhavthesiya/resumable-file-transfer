/*
 * checkpoint.h - the Enhanced client's durable progress record.
 *
 * HOW THIS WORKS
 * --------------
 * Case 2 needed one position.  Here K connections make progress independently,
 * so the record holds one sub-record per connection:
 *
 *     connection c is on work item `item`, and has `offset` bytes of it
 *
 * All K sub-records live in ONE file, <output_dir>/<topic>/.checkpoint, written
 * with the same tmp-plus-rename trick as Case 2 so a reader can never see a
 * half-written record.  Keeping them in one file rather than K files is
 * deliberate: a single rename() makes the whole set of K positions atomic
 * together, whereas K separate renames could be interrupted midway and leave
 * the connections describing inconsistent moments.
 *
 * The K writers are serialised by a mutex the caller holds, since several
 * connection threads reach a checkpoint at once.
 *
 * The record also stores K and the split threshold.  Both are needed to know
 * that a resumed run rebuilt the identical work plan - if either changed, the
 * item indices would mean something different, and the record is discarded.
 */
#ifndef IT305_ECHECKPOINT_H
#define IT305_ECHECKPOINT_H

#include "../common/common.h"

typedef struct {
    size_t   item;        /* index into this connection's item list    */
    uint64_t offset;      /* absolute file offset reached in that item */
} conn_progress_t;

typedef struct {
    uint64_t        session_id;
    char            topic[MAX_KEY_LEN];
    uint32_t        file_count;
    unsigned        k;
    uint64_t        split_threshold;
    conn_progress_t conn[MAX_CONNS_PER_SESSION];
} echeckpoint_t;

/* Write the record atomically.  Returns 0 on success, -1 on failure. */
int  echeckpoint_save(const char *dir, const echeckpoint_t *cp, int do_fsync);

/* Read it back.  Returns 0 on success, -1 if missing, malformed, or failing
 * its CRC - all of which simply mean "start a fresh session". */
int  echeckpoint_load(const char *dir, echeckpoint_t *cp);

void echeckpoint_remove(const char *dir);

#endif /* IT305_ECHECKPOINT_H */
