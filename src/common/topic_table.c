#include "common.h"
#include "crc32.h"
#include "logging.h"
#include "protocol.h"   /* name_safe_topic, name_safe_file */
#include "topic_table.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

/* Copy a NUL-terminated name into a fixed buffer, refusing to truncate.
 * Returns 0 on success, -1 if it would not fit.  Used instead of snprintf("%s")
 * so the bound is explicit in the code rather than something the compiler has
 * to infer - with -Werror=format-truncation, snprintf of a 255-byte d_name
 * into a 64-byte key is a build error even when an earlier check rules it out. */
static int copy_name(char *dst, size_t dstsz, const char *src)
{
    size_t n = strlen(src);
    if (n >= dstsz)
        return -1;
    memcpy(dst, src, n + 1);
    return 0;
}

/* scandir() filter: drop "." and "..", keep everything else.  The decision of
 * what is a directory or a regular file is made afterwards with stat(), not
 * from dirent.d_type, because d_type is DT_UNKNOWN on some filesystems. */
static int not_dot(const struct dirent *e)
{
    return !(strcmp(e->d_name, ".") == 0 || strcmp(e->d_name, "..") == 0);
}

static void free_namelist(struct dirent **list, int n)
{
    for (int i = 0; i < n; i++)
        free(list[i]);
    free(list);
}

/* Populate one topic's file list.  Returns 0 on success, -1 on failure. */
static int scan_topic_files(topic_t *tp, int with_crc)
{
    struct dirent **names = NULL;
    int n = scandir(tp->dir_path, &names, not_dot, alphasort);
    if (n < 0) {
        log_errno(LOG_ERROR, tp->dir_path);
        return -1;
    }

    /* n is an upper bound: sub-directories and specials are skipped below. */
    tp->files = calloc((size_t)n ? (size_t)n : 1, sizeof *tp->files);
    if (!tp->files) {
        free_namelist(names, n);
        log_error("out of memory building file list for topic %s", tp->key);
        return -1;
    }

    tp->n_files     = 0;
    tp->total_bytes = 0;

    for (int i = 0; i < n; i++) {
        char path[MAX_PATH_LEN];
        if ((size_t)snprintf(path, sizeof path, "%s/%s", tp->dir_path,
                             names[i]->d_name) >= sizeof path) {
            log_warn("skipping %s/%s: path too long", tp->dir_path, names[i]->d_name);
            continue;
        }

        struct stat st;
        if (stat(path, &st) < 0) {
            log_warn("skipping %s: %s", path, strerror(errno));
            continue;
        }
        if (!S_ISREG(st.st_mode))
            continue;                         /* directories, symlinks, FIFOs */

        /* A file whose name the protocol could not carry safely is excluded
         * from the catalogue entirely, rather than being offered and then
         * rejected when the client tries to create it. */
        if (name_safe_file(names[i]->d_name) < 0) {
            log_warn("skipping %s: name is not protocol-safe", path);
            continue;
        }

        file_entry_t *fe = &tp->files[tp->n_files];
        if (copy_name(fe->name, sizeof fe->name, names[i]->d_name) < 0) {
            log_warn("skipping %s: name too long for the protocol", path);
            continue;
        }
        fe->size  = (uint64_t)st.st_size;
        fe->crc32 = 0;
        if (with_crc && crc32_file(path, &fe->crc32) < 0) {
            log_warn("skipping %s: cannot read for checksum: %s",
                     path, strerror(errno));
            continue;
        }
        tp->total_bytes += fe->size;
        tp->n_files++;
    }

    free_namelist(names, n);
    return 0;
}

int topic_table_build(topic_table_t *t, const char *root, int with_crc)
{
    memset(t, 0, sizeof *t);

    struct stat rst;
    if (stat(root, &rst) < 0) {
        log_error("topics_root_dir %s: %s", root, strerror(errno));
        return -1;
    }
    if (!S_ISDIR(rst.st_mode)) {
        log_error("topics_root_dir %s is not a directory", root);
        return -1;
    }

    struct dirent **names = NULL;
    int n = scandir(root, &names, not_dot, alphasort);
    if (n < 0) {
        log_errno(LOG_ERROR, root);
        return -1;
    }

    double t0 = mono_now();
    for (int i = 0; i < n; i++) {
        if (t->n_topics >= MAX_TOPICS) {
            log_warn("more than %d topics in %s; ignoring the rest",
                     MAX_TOPICS, root);
            break;
        }

        char path[MAX_PATH_LEN];
        if ((size_t)snprintf(path, sizeof path, "%s/%s", root,
                             names[i]->d_name) >= sizeof path) {
            log_warn("skipping topic %s: path too long", names[i]->d_name);
            continue;
        }

        struct stat st;
        if (stat(path, &st) < 0 || !S_ISDIR(st.st_mode))
            continue;                          /* only directories are topics */

        if (name_safe_topic(names[i]->d_name) < 0) {
            log_warn("skipping topic directory %s: name is not protocol-safe",
                     names[i]->d_name);
            continue;
        }

        topic_t *tp = &t->topics[t->n_topics];
        if (copy_name(tp->key, sizeof tp->key, names[i]->d_name) < 0 ||
            copy_name(tp->dir_path, sizeof tp->dir_path, path) < 0) {
            log_warn("skipping topic %s: name or path too long", names[i]->d_name);
            continue;
        }
        if (scan_topic_files(tp, with_crc) < 0) {
            free_namelist(names, n);
            topic_table_free(t);
            return -1;
        }
        t->total_bytes += tp->total_bytes;
        t->total_files += tp->n_files;
        t->n_topics++;
    }
    free_namelist(names, n);

    if (t->n_topics == 0) {
        log_error("no topic sub-directories found in %s", root);
        return -1;
    }
    log_info("topic table: %zu topics, %zu files, %.1f MB, built in %.2f s "
             "(crc32 %s)",
             t->n_topics, t->total_files, (double)t->total_bytes / 1048576.0,
             mono_now() - t0, with_crc ? "computed" : "skipped");
    return 0;
}

const topic_t *topic_table_lookup(const topic_table_t *t, const char *key)
{
    /* Linear scan: with at most a few tens of topics this is a handful of
     * string compares, far cheaper than the file transfer that follows, and it
     * keeps the structure (and its complete absence of locking) obvious. */
    for (size_t i = 0; i < t->n_topics; i++)
        if (strcmp(t->topics[i].key, key) == 0)
            return &t->topics[i];
    return NULL;
}

int topic_file_path(char *out, size_t outsz, const topic_t *tp, size_t idx)
{
    if (idx >= tp->n_files)
        return -1;
    int k = snprintf(out, outsz, "%s/%s", tp->dir_path, tp->files[idx].name);
    return (k < 0 || (size_t)k >= outsz) ? -1 : 0;
}

void topic_table_log(const topic_table_t *t)
{
    for (size_t i = 0; i < t->n_topics; i++)
        log_info("  topic %-16s %6zu files  %10.2f MB  %s",
                 t->topics[i].key, t->topics[i].n_files,
                 (double)t->topics[i].total_bytes / 1048576.0,
                 t->topics[i].dir_path);
}

void topic_table_free(topic_table_t *t)
{
    for (size_t i = 0; i < t->n_topics; i++) {
        free(t->topics[i].files);
        t->topics[i].files = NULL;
    }
    t->n_topics = 0;
}
