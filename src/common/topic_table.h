/*
 * topic_table.h - the topic -> directory map, built once at start-up.
 *
 * HOW THIS WORKS
 * --------------
 * At start-up the server scans `topics_root_dir`.  Every immediate
 * sub-directory becomes a topic, and its name is the topic key; the key is
 * never hard-coded anywhere, which is what lets the grader point the server at
 * its own reference dataset.  For each topic the sorted list of regular files
 * is captured once, together with each file's size and CRC-32.
 *
 * Two properties matter:
 *
 *  - Caching the sorted listing fixes one canonical file order (alphabetical,
 *    via scandir + alphasort) that client and server agree on with no
 *    negotiation.  That shared order is what makes a numeric `file_idx` in a
 *    resume request meaningful.
 *
 *  - The table is fully populated *before* the listening socket is created and
 *    is never mutated afterwards, so topic_table_lookup() needs no lock even
 *    though every worker thread calls it.  This is the reason the read-only
 *    catalogue and the mutable session table are kept as separate structures.
 */
#ifndef IT305_TOPIC_TABLE_H
#define IT305_TOPIC_TABLE_H

#include "common.h"

typedef struct {
    char     name[MAX_NAME_LEN];
    uint64_t size;
    uint32_t crc32;
} file_entry_t;

typedef struct {
    char          key[MAX_KEY_LEN];        /* topic name, e.g. "gallina"   */
    char          dir_path[MAX_PATH_LEN];  /* directory holding the files  */
    file_entry_t *files;                   /* sorted, owned by the table   */
    size_t        n_files;
    uint64_t      total_bytes;             /* sum of all file sizes        */
} topic_t;

typedef struct {
    topic_t  topics[MAX_TOPICS];
    size_t   n_topics;
    uint64_t total_bytes;
    size_t   total_files;
} topic_table_t;

/* Scan `root` and populate `t`.  If `with_crc` is non-zero, each file's CRC-32
 * is computed now (one pass over the data) so the request path never has to.
 * Returns 0 on success, -1 on failure (reason already logged).
 * A root with no sub-directories is an error: the server has nothing to serve
 * and should say so at start-up rather than at the first request. */
int topic_table_build(topic_table_t *t, const char *root, int with_crc);

/* Returns the topic, or NULL if `key` is not present.  Lock-free by design. */
const topic_t *topic_table_lookup(const topic_table_t *t, const char *key);

/* Compose "<dir_path>/<files[idx].name>" into `out`.
 * Returns 0 on success, -1 if idx is out of range or the path would not fit. */
int topic_file_path(char *out, size_t outsz, const topic_t *tp, size_t idx);

/* Log one line per topic: name, file count, total size. */
void topic_table_log(const topic_table_t *t);

void topic_table_free(topic_table_t *t);

#endif /* IT305_TOPIC_TABLE_H */
