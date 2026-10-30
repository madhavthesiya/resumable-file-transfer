#include "workplan.h"

#include "../common/logging.h"

#include <stdlib.h>
#include <string.h>

static int il_push(itemlist_t *il, uint32_t file_idx, uint64_t start, uint64_t end)
{
    if (il->n == il->cap) {
        size_t cap = il->cap ? il->cap * 2 : 64;
        work_item_t *p = realloc(il->items, cap * sizeof *p);
        if (!p)
            return -1;
        il->items = p;
        il->cap   = cap;
    }
    il->items[il->n].file_idx = file_idx;
    il->items[il->n].start    = start;
    il->items[il->n].end      = end;
    il->n++;
    return 0;
}

int workplan_build(workplan_t *wp, const file_meta_t *files, size_t n_files,
                   unsigned k, uint64_t split_threshold)
{
    if (k == 0 || k > MAX_CONNS_PER_SESSION)
        return -1;

    memset(wp, 0, sizeof *wp);
    wp->k               = k;
    wp->split_threshold = split_threshold;

    for (size_t i = 0; i < n_files; i++) {
        uint64_t size = files[i].size;
        if (size == 0)
            continue;              /* nothing to transfer; pre-created instead */

        if (size < split_threshold || k == 1) {
            /* Whole file on one connection, chosen by index so consecutive
             * files land on different connections. */
            if (il_push(&wp->conn[i % k], (uint32_t)i, 0, size) < 0)
                goto oom;
            wp->total_items++;
        } else {
            /* Cut into K contiguous ranges.  Computing each boundary as
             * size*j/k keeps the pieces exact with no leftover byte, and makes
             * range j depend only on j, so the split is reproducible. */
            for (unsigned j = 0; j < k; j++) {
                uint64_t start = size * (uint64_t)j / k;
                uint64_t end   = size * (uint64_t)(j + 1) / k;
                if (end <= start)
                    continue;      /* possible when size < k */
                if (il_push(&wp->conn[j], (uint32_t)i, start, end) < 0)
                    goto oom;
                wp->total_items++;
            }
            wp->n_split_files++;
        }
        wp->total_bytes += size;
    }
    return 0;

oom:
    log_error("out of memory building the work plan");
    workplan_free(wp);
    return -1;
}

void workplan_free(workplan_t *wp)
{
    for (unsigned j = 0; j < MAX_CONNS_PER_SESSION; j++) {
        free(wp->conn[j].items);
        wp->conn[j].items = NULL;
        wp->conn[j].n = wp->conn[j].cap = 0;
    }
}

void workplan_log(const workplan_t *wp)
{
    log_info("work plan: %zu item(s) over %u connection(s), %.2f MB total, "
             "%zu file(s) range-split at the %llu byte threshold",
             wp->total_items, wp->k, (double)wp->total_bytes / 1048576.0,
             wp->n_split_files, (unsigned long long)wp->split_threshold);
    for (unsigned j = 0; j < wp->k; j++) {
        uint64_t bytes = 0;
        for (size_t i = 0; i < wp->conn[j].n; i++)
            bytes += wp->conn[j].items[i].end - wp->conn[j].items[i].start;
        log_info("  conn %u: %6zu item(s), %8.2f MB", j, wp->conn[j].n,
                 (double)bytes / 1048576.0);
    }
}
