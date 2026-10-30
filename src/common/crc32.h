/*
 * crc32.h - CRC-32 (IEEE 802.3 / zlib polynomial 0xEDB88320).
 *
 * HOW THIS WORKS
 * --------------
 * A 256-entry lookup table is built once on first use; crc32_update() then
 * folds one byte at a time.  The server computes each file's CRC once at
 * start-up (see topic_table.c) and sends it in RESP_FILE_INFO; the client
 * recomputes it over the bytes it actually wrote to disk and compares.  That is
 * an end-to-end integrity check independent of "the byte count matched".
 *
 * crc32_init(0) -> feed bytes with crc32_update() -> crc32_final() to finish.
 */
#ifndef IT305_CRC32_H
#define IT305_CRC32_H

#include <stddef.h>
#include <stdint.h>

/* Starting value for a fresh CRC computation. */
#define CRC32_INIT 0xFFFFFFFFu

/* Fold `len` bytes into the running CRC and return the new running value. */
uint32_t crc32_update(uint32_t running, const void *buf, size_t len);

/* Convert a running value into the final CRC-32 to put on the wire. */
static inline uint32_t crc32_final(uint32_t running) { return running ^ 0xFFFFFFFFu; }

/* One-shot convenience wrapper for a complete buffer. */
uint32_t crc32_buf(const void *buf, size_t len);

/* CRC-32 of a whole file, read in blocks.  Returns 0 and stores the result in
 * *out on success; returns -1 with errno set if the file could not be read. */
int crc32_file(const char *path, uint32_t *out);

#endif /* IT305_CRC32_H */
