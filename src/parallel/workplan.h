/*
 * workplan.h - splitting a topic across K parallel connections.
 *
 * HOW THIS WORKS
 * --------------
 * The client fetches the whole file list once, then turns it into a list of
 * *work items*.  A work item is a byte range of one file:
 *
 *     (file_idx, start, end)      with end exclusive
 *
 * and every item is assigned to exactly one of the K connections:
 *
 *   - a file smaller than `split_threshold` becomes ONE item covering the whole
 *     file, assigned to connection `file_idx mod K`;
 *   - a file at or above the threshold is cut into K contiguous ranges, and
 *     range j goes to connection j.
 *
 * WHY THE FILE LIST IS PARTITIONED AT ALL
 * ---------------------------------------
 * The design document proposed splitting only large files by byte range.  On
 * this dataset that would achieve almost nothing: of the 26,179 files in
 * Animals-10 only 44 are 512 KiB or larger, and 43 of those sit in a single
 * topic, so nine topics out of ten contain no splittable file whatsoever.
 * Round-robin assignment of whole files is what actually keeps K connections
 * busy here, and range splitting is kept as a second mechanism for the few
 * genuinely large files.  README records this deviation and its justification.
 *
 * A zero-length file produces no work item: there is nothing to transfer.  The
 * client still creates it, because every output file is pre-created at its
 * final size before any connection starts (which is also what lets the K
 * connections pwrite() into the same file at different offsets without
 * coordinating).
 *
 * Item order is deterministic - it follows the server's alphabetical file list
 * and the fixed assignment rule above - so a client that restarts and refetches
 * the same list rebuilds exactly the same plan.  That is what makes a saved
 * "connection c is on item i, offset o" checkpoint meaningful across restarts.
 */
#ifndef IT305_WORKPLAN_H
#define IT305_WORKPLAN_H

#include "../common/common.h"
#include "../common/protocol.h"   /* file_meta_t */

typedef struct {
    uint32_t file_idx;
    uint64_t start;
    uint64_t end;        /* exclusive */
} work_item_t;

typedef struct {
    work_item_t *items;
    size_t       n;
    size_t       cap;
} itemlist_t;

typedef struct {
    itemlist_t conn[MAX_CONNS_PER_SESSION];
    unsigned   k;
    uint64_t   split_threshold;
    size_t     total_items;
    size_t     n_split_files;     /* how many files were range-split  */
    uint64_t   total_bytes;       /* sum of all item lengths          */
} workplan_t;

/* Build the plan.  Returns 0 on success, -1 on allocation failure or bad
 * arguments (k must be 1..MAX_CONNS_PER_SESSION). */
int  workplan_build(workplan_t *wp, const file_meta_t *files, size_t n_files,
                    unsigned k, uint64_t split_threshold);

void workplan_free(workplan_t *wp);

/* Log a one-line summary per connection, so a run's log explains how the work
 * was divided. */
void workplan_log(const workplan_t *wp);

#endif /* IT305_WORKPLAN_H */
