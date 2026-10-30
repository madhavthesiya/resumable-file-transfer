#include "checkpoint.h"

#include "../common/crc32.h"
#include "../common/logging.h"
#include "../common/protocol.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define ECKPT_MAGIC "IT305-ECKPT1"

static void paths_for(const char *dir, char *final_path, char *tmp_path, size_t cap)
{
    snprintf(final_path, cap, "%s/.checkpoint", dir);
    snprintf(tmp_path,   cap, "%s/.checkpoint.tmp", dir);
}

int echeckpoint_save(const char *dir, const echeckpoint_t *cp, int do_fsync)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);

    /* Render the whole body first so the CRC covers exactly what is stored. */
    char body[256 + MAX_CONNS_PER_SESSION * 64];
    int used = snprintf(body, sizeof body,
                        "magic=%s\n"
                        "session=%016llx\n"
                        "topic=%s\n"
                        "file_count=%u\n"
                        "conns=%u\n"
                        "split=%llu\n",
                        ECKPT_MAGIC, (unsigned long long)cp->session_id,
                        cp->topic, cp->file_count, cp->k,
                        (unsigned long long)cp->split_threshold);
    if (used < 0 || (size_t)used >= sizeof body)
        return -1;

    for (unsigned j = 0; j < cp->k; j++) {
        int n = snprintf(body + used, sizeof body - (size_t)used,
                         "c%u=%zu %llu\n", j, cp->conn[j].item,
                         (unsigned long long)cp->conn[j].offset);
        if (n < 0 || (size_t)(used + n) >= sizeof body) {
            log_error("checkpoint record does not fit in its buffer");
            return -1;
        }
        used += n;
    }

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        log_errno(LOG_WARN, tmp_path);
        return -1;
    }
    uint32_t crc = crc32_buf(body, (size_t)used);
    int ok = (dprintf(fd, "%s", body) >= 0) &&
             (dprintf(fd, "crc32=%08x\n", crc) >= 0);
    if (ok && do_fsync && fsync(fd) < 0)
        ok = 0;
    if (close(fd) < 0)
        ok = 0;
    if (!ok) {
        unlink(tmp_path);
        return -1;
    }
    /* The atomic step. */
    if (rename(tmp_path, final_path) < 0) {
        log_errno(LOG_WARN, "rename of the checkpoint into place");
        unlink(tmp_path);
        return -1;
    }
    return 0;
}

int echeckpoint_load(const char *dir, echeckpoint_t *cp)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);

    FILE *f = fopen(final_path, "r");
    if (!f)
        return -1;
    char buf[512 + MAX_CONNS_PER_SESSION * 64];
    size_t len = fread(buf, 1, sizeof buf - 1, f);
    int oversized = !feof(f);
    fclose(f);
    if (oversized) {
        log_warn("checkpoint %s is larger than expected; ignoring it", final_path);
        return -1;
    }
    buf[len] = '\0';

    char *crc_line = strstr(buf, "\ncrc32=");
    if (!crc_line) {
        log_warn("checkpoint %s has no crc32 line; ignoring it", final_path);
        return -1;
    }
    size_t body_len = (size_t)(crc_line - buf) + 1;
    unsigned stored = 0;
    if (sscanf(crc_line + 1, "crc32=%8x", &stored) != 1) {
        log_warn("checkpoint %s has a malformed crc32 line; ignoring it", final_path);
        return -1;
    }
    if (crc32_buf(buf, body_len) != stored) {
        log_warn("checkpoint %s failed its CRC check; ignoring it", final_path);
        return -1;
    }

    memset(cp, 0, sizeof *cp);
    unsigned long long sid = 0, split = 0;
    unsigned fc = 0, k = 0;
    char magic[32] = "", topic[MAX_KEY_LEN] = "";
    if (sscanf(buf, "magic=%31s\nsession=%llx\ntopic=%63s\nfile_count=%u\n"
                    "conns=%u\nsplit=%llu\n",
               magic, &sid, topic, &fc, &k, &split) != 6) {
        log_warn("checkpoint %s is malformed; ignoring it", final_path);
        return -1;
    }
    if (strcmp(magic, ECKPT_MAGIC) != 0 || sid == 0 ||
        name_safe_topic(topic) < 0 || k == 0 || k > MAX_CONNS_PER_SESSION) {
        log_warn("checkpoint %s is internally inconsistent; ignoring it", final_path);
        return -1;
    }

    cp->session_id      = (uint64_t)sid;
    snprintf(cp->topic, sizeof cp->topic, "%s", topic);
    cp->file_count      = fc;
    cp->k               = k;
    cp->split_threshold = (uint64_t)split;

    /* Each per-connection line is found by its own key, so their order does
     * not matter and a missing one is detected rather than defaulted. */
    for (unsigned j = 0; j < k; j++) {
        char key[16];
        snprintf(key, sizeof key, "\nc%u=", j);
        char *at = strstr(buf, key);
        unsigned long long item = 0, off = 0;
        if (!at || sscanf(at + strlen(key), "%llu %llu", &item, &off) != 2) {
            log_warn("checkpoint %s has no usable record for connection %u; "
                     "ignoring it", final_path, j);
            return -1;
        }
        cp->conn[j].item   = (size_t)item;
        cp->conn[j].offset = (uint64_t)off;
    }
    return 0;
}

void echeckpoint_remove(const char *dir)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);
    unlink(final_path);
    unlink(tmp_path);
}
