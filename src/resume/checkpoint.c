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

#define CKPT_MAGIC "IT305-CKPT1"

static void paths_for(const char *dir, char *final_path, char *tmp_path, size_t cap)
{
    snprintf(final_path, cap, "%s/.checkpoint", dir);
    snprintf(tmp_path,   cap, "%s/.checkpoint.tmp", dir);
}

int checkpoint_save(const char *dir, const checkpoint_t *cp, int do_fsync)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);

    /* Render the body first, so its CRC can be appended as the last line. */
    char body[512];
    int n = snprintf(body, sizeof body,
                     "magic=%s\n"
                     "session=%016llx\n"
                     "topic=%s\n"
                     "file_count=%u\n"
                     "file_idx=%u\n"
                     "byte_offset=%llu\n",
                     CKPT_MAGIC, (unsigned long long)cp->session_id, cp->topic,
                     cp->file_count, cp->file_idx,
                     (unsigned long long)cp->byte_offset);
    if (n < 0 || (size_t)n >= sizeof body) {
        log_error("checkpoint record does not fit in its buffer");
        return -1;
    }

    int fd = open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        log_errno(LOG_WARN, tmp_path);
        return -1;
    }
    uint32_t crc = crc32_buf(body, (size_t)n);
    int ok = (dprintf(fd, "%s", body) >= 0) &&
             (dprintf(fd, "crc32=%08x\n", crc) >= 0);
    if (ok && do_fsync && fsync(fd) < 0) {
        log_errno(LOG_WARN, "fsync of the checkpoint");
        ok = 0;
    }
    if (close(fd) < 0)
        ok = 0;
    if (!ok) {
        unlink(tmp_path);
        return -1;
    }

    /* The atomic step: once this returns, .checkpoint is wholly the new
     * record, whatever happens next. */
    if (rename(tmp_path, final_path) < 0) {
        log_errno(LOG_WARN, "rename of the checkpoint into place");
        unlink(tmp_path);
        return -1;
    }
    return 0;
}

int checkpoint_load(const char *dir, checkpoint_t *cp)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);

    FILE *f = fopen(final_path, "r");
    if (!f)
        return -1;                      /* no checkpoint: a fresh start */

    char buf[768];
    size_t len = fread(buf, 1, sizeof buf - 1, f);
    int oversized = !feof(f);
    fclose(f);
    if (oversized) {
        log_warn("checkpoint %s is larger than expected; ignoring it", final_path);
        return -1;
    }
    buf[len] = '\0';

    /* The CRC line must be last, and it covers everything before it. */
    char *crc_line = strstr(buf, "\ncrc32=");
    if (!crc_line) {
        log_warn("checkpoint %s has no crc32 line; ignoring it", final_path);
        return -1;
    }
    size_t body_len = (size_t)(crc_line - buf) + 1;   /* include that newline */
    unsigned stored = 0;
    if (sscanf(crc_line + 1, "crc32=%8x", &stored) != 1) {
        log_warn("checkpoint %s has a malformed crc32 line; ignoring it", final_path);
        return -1;
    }
    uint32_t actual = crc32_buf(buf, body_len);
    if (actual != stored) {
        log_warn("checkpoint %s failed its CRC check (%08x vs %08x); ignoring it",
                 final_path, actual, stored);
        return -1;
    }

    memset(cp, 0, sizeof *cp);
    unsigned long long sid = 0, off = 0;
    unsigned fc = 0, fi = 0;
    char magic[32] = "", topic[MAX_KEY_LEN] = "";
    /* The %31s and %63s widths bound the scans to their destination buffers. */
    if (sscanf(buf,
               "magic=%31s\nsession=%llx\ntopic=%63s\nfile_count=%u\n"
               "file_idx=%u\nbyte_offset=%llu\n",
               magic, &sid, topic, &fc, &fi, &off) != 6) {
        log_warn("checkpoint %s is malformed; ignoring it", final_path);
        return -1;
    }
    if (strcmp(magic, CKPT_MAGIC) != 0) {
        log_warn("checkpoint %s has magic '%s', expected '%s'; ignoring it",
                 final_path, magic, CKPT_MAGIC);
        return -1;
    }
    /* Internal consistency: a session id of 0 is reserved, the topic must be
     * path-safe, and the position cannot be past the end of the file list. */
    if (sid == 0 || name_safe_topic(topic) < 0 || fi > fc) {
        log_warn("checkpoint %s is internally inconsistent; ignoring it", final_path);
        return -1;
    }

    cp->session_id  = (uint64_t)sid;
    snprintf(cp->topic, sizeof cp->topic, "%s", topic);
    cp->file_count  = fc;
    cp->file_idx    = fi;
    cp->byte_offset = (uint64_t)off;
    return 0;
}

void checkpoint_remove(const char *dir)
{
    char final_path[MAX_PATH_LEN], tmp_path[MAX_PATH_LEN];
    paths_for(dir, final_path, tmp_path, sizeof final_path);
    unlink(final_path);
    unlink(tmp_path);
}
