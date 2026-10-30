#include "common.h"
#include "crc32.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

/*
 * HOW THIS WORKS - "slicing by 8"
 * -------------------------------
 * The textbook CRC-32 folds one byte per iteration through a single 256-entry
 * table.  That is simple but slow: measured on this project's dataset it ran at
 * about 42 MB/s, so checksumming the 586 MB Animals-10 set at server start-up
 * took 13.9 seconds.
 *
 * Slicing by 8 keeps the same polynomial and produces bit-identical results,
 * but consumes 8 bytes per iteration using 8 precomputed tables.  Table t
 * answers "what does a byte contribute if it is t positions from the end of the
 * window", which is exactly the recurrence
 *
 *     tab[t][i] = (tab[t-1][i] >> 8) ^ tab[0][tab[t-1][i] & 0xFF]
 *
 * The 8 table lookups in one iteration are independent, so the CPU can issue
 * them in parallel instead of serialising on a one-byte-at-a-time dependency
 * chain.  The tables cost 8 KiB of memory, built once.
 *
 * The bytes are assembled into 32-bit words with explicit shifts rather than by
 * casting the pointer to uint32_t*: that keeps the result identical on any
 * endianness and avoids both unaligned loads and strict-aliasing problems.
 */

#define CRC32_POLY_REFLECTED 0xEDB88320u

static uint32_t g_tab[8][256];
static pthread_once_t g_once = PTHREAD_ONCE_INIT;

static void build_tables(void)
{
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1u) ? (CRC32_POLY_REFLECTED ^ (c >> 1)) : (c >> 1);
        g_tab[0][i] = c;
    }
    for (uint32_t i = 0; i < 256; i++)
        for (int t = 1; t < 8; t++)
            g_tab[t][i] = (g_tab[t - 1][i] >> 8) ^ g_tab[0][g_tab[t - 1][i] & 0xFFu];
}

uint32_t crc32_update(uint32_t crc, const void *buf, size_t len)
{
    /* pthread_once() makes the table build safe even if several worker threads
     * reach their first checksum simultaneously. */
    pthread_once(&g_once, build_tables);

    const unsigned char *p = (const unsigned char *)buf;

    while (len >= 8) {
        uint32_t lo = (uint32_t)p[0]        | ((uint32_t)p[1] << 8)
                    | ((uint32_t)p[2] << 16)| ((uint32_t)p[3] << 24);
        uint32_t hi = (uint32_t)p[4]        | ((uint32_t)p[5] << 8)
                    | ((uint32_t)p[6] << 16)| ((uint32_t)p[7] << 24);
        lo ^= crc;
        crc = g_tab[7][ lo        & 0xFFu] ^ g_tab[6][(lo >>  8) & 0xFFu]
            ^ g_tab[5][(lo >> 16) & 0xFFu] ^ g_tab[4][(lo >> 24) & 0xFFu]
            ^ g_tab[3][ hi        & 0xFFu] ^ g_tab[2][(hi >>  8) & 0xFFu]
            ^ g_tab[1][(hi >> 16) & 0xFFu] ^ g_tab[0][(hi >> 24) & 0xFFu];
        p   += 8;
        len -= 8;
    }
    /* Tail: fewer than 8 bytes left, so fall back to one byte at a time. */
    while (len-- > 0)
        crc = g_tab[0][(crc ^ *p++) & 0xFFu] ^ (crc >> 8);

    return crc;
}

uint32_t crc32_buf(const void *buf, size_t len)
{
    return crc32_final(crc32_update(CRC32_INIT, buf, len));
}

int crc32_file(const char *path, uint32_t *out)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    /* 256 KiB blocks: big enough that the read() syscall cost disappears next
     * to the checksum work, small enough to stay out of the way. */
    static const size_t BLK = 256 * 1024;
    unsigned char *blk = malloc(BLK);
    if (!blk) {
        close(fd);
        errno = ENOMEM;
        return -1;
    }

    uint32_t run = CRC32_INIT;
    for (;;) {
        ssize_t n = read(fd, blk, BLK);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            free(blk);
            close(fd);
            return -1;
        }
        if (n == 0)
            break;
        run = crc32_update(run, blk, (size_t)n);
    }
    free(blk);
    close(fd);
    *out = crc32_final(run);
    return 0;
}
