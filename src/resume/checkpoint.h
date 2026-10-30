/*
 * checkpoint.h - the client's durable record of transfer progress.
 *
 * HOW THIS WORKS
 * --------------
 * The client is the side that must survive its own restart between a fault and
 * the resume, so the client is the side that writes progress to stable storage.
 * The record lives at <output_dir>/<topic>/.checkpoint and is rewritten every
 * 64 KiB of progress and at every file boundary.
 *
 * WRITING IT ATOMICALLY
 * ---------------------
 * The new record is written to .checkpoint.tmp and then rename()d over
 * .checkpoint.  rename() within one filesystem is atomic, so a reader sees
 * either the whole old record or the whole new one - never a half-written
 * mixture.  Writing in place would risk exactly that: a client killed partway
 * through the write would leave a record whose offset belongs to one moment and
 * whose file index belongs to another, which is worse than having no record.
 *
 * By default the temporary file is not fsync()ed before the rename.  That makes
 * the record robust against the client process being killed - which is the
 * failure this project injects and tests - but not against the machine losing
 * power, and it avoids roughly one fsync per 64 KiB (about 940 of them on a
 * 60 MB topic).  Pass a non-zero do_fsync to trade the speed for power-loss
 * durability.
 *
 * THE POSITION IT STORES
 * ----------------------
 * (file_idx, byte_offset) means: every file below file_idx is complete on disk,
 * and the first byte_offset bytes of file_idx are on disk.  PROTOCOL.md section
 * 5.2.1 explains why that convention and not "(last file, its length)".
 *
 * The file is plain text with one key per line, so it can be read aloud during
 * a demonstration, and it ends with a CRC-32 over everything above it, so a
 * truncated or corrupted record is detected and ignored rather than trusted.
 */
#ifndef IT305_CHECKPOINT_H
#define IT305_CHECKPOINT_H

#include "../common/common.h"

typedef struct {
    uint64_t session_id;
    char     topic[MAX_KEY_LEN];
    uint32_t file_count;
    uint32_t file_idx;
    uint64_t byte_offset;
} checkpoint_t;

/* Write `cp` into <dir>/.checkpoint atomically.
 * Returns 0 on success, -1 on failure (reason logged). */
int checkpoint_save(const char *dir, const checkpoint_t *cp, int do_fsync);

/* Read <dir>/.checkpoint.  Returns 0 on success, -1 if it is missing,
 * unreadable, malformed, or fails its CRC check.  A -1 return simply means
 * "start a fresh session", so a corrupt record is never fatal. */
int checkpoint_load(const char *dir, checkpoint_t *cp);

/* Delete the checkpoint (and any stale temporary file).  Called once the
 * transfer completes, and whenever the server refuses a resume. */
void checkpoint_remove(const char *dir);

#endif /* IT305_CHECKPOINT_H */
