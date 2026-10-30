/*
 * protocol.h - the wire protocol of PROTOCOL.md, in code.
 *
 * HOW THIS WORKS
 * --------------
 * Three layers, each built on the one below:
 *
 *  1. io_send_all / io_recv_all - loop over short send()/recv() returns and
 *     retry EINTR, so callers never have to.  Both optionally add the bytes
 *     they moved to a counter, which is how every metric in the report is
 *     collected without sprinkling arithmetic through the transfer loops.
 *
 *  2. msg_send / msg_recv_hdr / msg_recv_payload - the 6-byte framing of
 *     PROTOCOL.md section 2.  msg_send_v() emits header + up to two payload
 *     pieces with a single writev(), which matters on the data path: one
 *     syscall per 16 KiB chunk instead of two, and the header can never be
 *     delivered in a separate TCP segment from its data.
 *
 *  3. send_* / parse_* - one typed function per message type.  Server and
 *     client code then reads like the message tables in PROTOCOL.md, and the
 *     byte layout exists in exactly one place.
 *
 * Packing and parsing go through the pb_t writer and pv_t reader below.  The
 * reader records an underflow in a flag instead of returning an error from
 * every getter, so a parser can be written as straight-line code and checked
 * once at the end with pv_done().  That is what makes "a truncated or malformed
 * message can never read past the buffer" easy to verify by inspection.
 */
#ifndef IT305_PROTOCOL_H
#define IT305_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>

#include "common.h"   /* MAX_NAME_LEN, MAX_PAYLOAD_LEN */

/* ---- message types (PROTOCOL.md sections 4-6) ---------------------------- */

/* Part I, reused unchanged by Part II Case 1 */
#define MSG_REQ_TOPIC          0x01
#define MSG_RESP_META          0x02
#define MSG_RESP_FILE_INFO     0x03
#define MSG_RESP_FILE_CHUNK    0x04
#define MSG_RESP_DONE          0x05
#define MSG_RESP_ERROR         0x06
/* Part II Case 2 - session management */
#define MSG_REQ_SESSION_START  0x10
#define MSG_RESP_SESSION_ID    0x11
#define MSG_REQ_SESSION_RESUME 0x12
#define MSG_RESP_RESUME_ACK    0x13
#define MSG_RESP_CHECKPOINT    0x14
#define MSG_REQ_ACK            0x15
/* Part II Case 2 Enhanced - parallel connections */
#define MSG_REQ_RANGE          0x20
#define MSG_RESP_RANGE_ACK     0x21
#define MSG_REQ_FILE_LIST      0x22
#define MSG_RESP_FILE_LIST     0x23
#define MSG_REQ_ATTACH         0x24
#define MSG_RESP_ATTACH_ACK    0x25
#define MSG_RESP_RANGE_CHUNK   0x26
#define MSG_RESP_RANGE_DONE    0x27
#define MSG_REQ_RANGE_ACK      0x28

/* ---- flags (PROTOCOL.md section 3) -------------------------------------- */
#define FLAG_NONE     0x00
#define FLAG_MORE     0x01
#define FLAG_RESUMED  0x02

/* ---- error codes carried in RESP_ERROR (PROTOCOL.md section 4.1) -------- */
#define ERR_UNKNOWN_TOPIC  1
#define ERR_BAD_REQUEST    2
#define ERR_INVALID_NAME   3
#define ERR_IO             4
#define ERR_NO_SESSION     5
#define ERR_BAD_RANGE      6
#define ERR_INTERNAL       7

/* Sentinel in the start_file_idx field of RESP_RESUME_ACK: "resume refused". */
#define RESUME_REJECTED 0xFFFFFFFFu

#define MSG_HEADER_SIZE 6

/* Fixed payload sizes, for validating a header before reading the payload. */
#define SZ_RESP_META         4
#define SZ_RESP_SESSION_ID  12
#define SZ_REQ_RESUME       20
#define SZ_RESP_RESUME_ACK  20
#define SZ_RESP_CHECKPOINT  16
#define SZ_REQ_ACK          20
#define SZ_REQ_RANGE        28
#define SZ_RESP_RANGE_ACK   20
#define SZ_REQ_FILE_LIST     8
#define SZ_REQ_ATTACH       12
#define SZ_RESP_ATTACH_ACK  12
#define SZ_RESP_RANGE_DONE  28
#define SZ_REQ_RANGE_ACK    28
#define SZ_RANGE_CHUNK_PFX  12   /* file_idx(4) + abs_offset(8) */

/* ---- layer 1: byte-stream I/O ------------------------------------------- */

typedef enum {
    IO_OK    =  0,   /* the full requested byte count was transferred     */
    IO_EOF   =  1,   /* the peer closed the connection cleanly            */
    IO_AGAIN =  2,   /* nothing was waiting (msg_try_recv_hdr only)       */
    IO_ERR   = -1    /* a real error; errno is set                        */
} io_rc;

/* Both loop until `n` bytes have moved, retrying EINTR.  If `wire` is non-NULL
 * the number of bytes actually transferred is added to it. */
io_rc io_send_all(int fd, const void *buf, size_t n, uint64_t *wire);
io_rc io_recv_all(int fd, void *buf, size_t n, uint64_t *wire);

/* ---- layer 2: framing --------------------------------------------------- */

typedef struct {
    uint8_t  type;
    uint8_t  flags;
    uint32_t payload_len;
} msg_hdr_t;

io_rc msg_send(int fd, uint8_t type, uint8_t flags,
               const void *payload, uint32_t len, uint64_t *wire);

/* header + p1 + p2 in one writev(); either piece may be NULL with length 0. */
io_rc msg_send_v(int fd, uint8_t type, uint8_t flags,
                 const void *p1, uint32_t n1,
                 const void *p2, uint32_t n2, uint64_t *wire);

/* Reads and validates a header.  Returns IO_ERR with errno = EPROTO if
 * payload_len exceeds MAX_PAYLOAD_LEN, so an absurd length declared by a
 * confused or hostile peer can never become an allocation. */
io_rc msg_recv_hdr(int fd, msg_hdr_t *out, uint64_t *wire);
io_rc msg_recv_payload(int fd, void *buf, uint32_t len, uint64_t *wire);

/* Read and throw away `len` payload bytes (used when a message is well formed
 * but not wanted in the current state). */
io_rc msg_skip_payload(int fd, uint32_t len, uint64_t *wire);

/* Non-blocking poll for an inbound message header.
 *
 * Returns IO_AGAIN immediately if nothing at all is waiting; otherwise behaves
 * like msg_recv_hdr() (it finishes reading a partially arrived header, blocking
 * for the few remaining bytes, because a half-delivered header is in flight and
 * will complete).
 *
 * This is what lets a server that is busy streaming data also keep its receive
 * queue drained.  In Case 2 the client sends a small REQ_ACK every 64 KiB; if
 * the server never read them they would pile up in its receive buffer until the
 * client's send() blocked and both sides stalled.  Using MSG_DONTWAIT per call
 * rather than switching the socket to O_NONBLOCK keeps the data path a plain
 * blocking send(). */
io_rc msg_try_recv_hdr(int fd, msg_hdr_t *out, uint64_t *wire);

/* Human-readable names for logs; never NULL. */
const char *msg_type_name(uint8_t type);
const char *err_code_name(uint16_t code);

/* ---- payload packing (writer) ------------------------------------------- */

typedef struct {
    uint8_t *buf;
    size_t   cap;
    size_t   len;
    int      overflow;   /* set once a put would not fit */
} pb_t;

void pb_init (pb_t *b, void *buf, size_t cap);
void pb_u8   (pb_t *b, uint8_t  v);
void pb_u16  (pb_t *b, uint16_t v);
void pb_u32  (pb_t *b, uint32_t v);
void pb_u64  (pb_t *b, uint64_t v);
void pb_bytes(pb_t *b, const void *p, size_t n);
int  pb_ok   (const pb_t *b);   /* 0 if everything fit, -1 on overflow */

/* ---- payload parsing (reader) ------------------------------------------- */

typedef struct {
    const uint8_t *buf;
    size_t         len;
    size_t         off;
    int            bad;   /* set once a get ran past the end */
} pv_t;

void     pv_init(pv_t *v, const void *buf, size_t len);
uint8_t  pv_u8  (pv_t *v);
uint16_t pv_u16 (pv_t *v);
uint32_t pv_u32 (pv_t *v);
uint64_t pv_u64 (pv_t *v);
const uint8_t *pv_bytes(pv_t *v, size_t n);   /* NULL on underflow */

/* 0 if no underflow occurred and every byte was consumed, -1 otherwise.
 * Requiring full consumption is deliberate: a payload longer than the message
 * type defines is treated as malformed rather than silently ignored. */
int pv_done(const pv_t *v);

/* ---- name safety (PROTOCOL.md section 7.3) ------------------------------ */

/* 0 if the name is safe to join onto a directory path, -1 otherwise.  Rejects
 * the empty string, anything containing a slash, "." and "..", and anything
 * too long for the corresponding buffer limit. */
int name_safe_topic(const char *s);
int name_safe_file(const char *s);

/* ---- layer 3: typed senders --------------------------------------------- */

io_rc send_req_topic     (int fd, const char *topic, uint64_t *wire);
io_rc send_resp_meta     (int fd, uint32_t file_count, uint64_t *wire);
io_rc send_resp_file_info(int fd, uint8_t flags, const char *name,
                          uint64_t size, uint32_t crc, uint64_t *wire);
io_rc send_resp_done     (int fd, uint64_t *wire);
io_rc send_resp_error    (int fd, uint16_t code, const char *fmt, ...)
                          __attribute__((format(printf, 3, 4)));

io_rc send_req_session_start (int fd, const char *topic, uint64_t *wire);
io_rc send_resp_session_id   (int fd, uint64_t sid, uint32_t file_count,
                              uint64_t *wire);
io_rc send_req_session_resume(int fd, uint64_t sid, uint32_t file_idx,
                              uint64_t byte_offset, uint64_t *wire);
io_rc send_resp_resume_ack   (int fd, uint64_t sid, uint32_t start_file_idx,
                              uint64_t start_offset, uint64_t *wire);
io_rc send_resp_checkpoint   (int fd, uint64_t sid, uint32_t done,
                              uint32_t total, uint64_t *wire);
io_rc send_req_ack           (int fd, uint64_t sid, uint32_t file_idx,
                              uint64_t byte_offset, uint64_t *wire);

io_rc send_req_range      (int fd, uint64_t sid, uint32_t file_idx,
                           uint64_t start, uint64_t end, uint64_t *wire);
io_rc send_resp_range_ack (int fd, uint64_t sid, uint32_t file_idx,
                           uint64_t start, uint64_t *wire);
io_rc send_req_file_list  (int fd, uint64_t sid, uint64_t *wire);
io_rc send_req_attach     (int fd, uint64_t sid, uint32_t conn_index,
                           uint64_t *wire);
io_rc send_resp_attach_ack(int fd, uint64_t sid, uint32_t conn_index,
                           uint64_t *wire);
io_rc send_resp_range_done(int fd, uint64_t sid, uint32_t file_idx,
                           uint64_t start, uint64_t end, uint64_t *wire);
io_rc send_req_range_ack  (int fd, uint64_t sid, uint32_t file_idx,
                           uint64_t range_start, uint64_t abs_offset,
                           uint64_t *wire);

/* ---- layer 3: typed parsers -------------------------------------------- */
/* Each returns 0 on success, -1 if the payload is malformed for that type. */

int parse_topic_name    (const void *p, uint32_t len, char *out, size_t outsz);
int parse_u32_only      (const void *p, uint32_t len, uint32_t *v);
int parse_resp_file_info(const void *p, uint32_t len, char *name, size_t namesz,
                         uint64_t *size, uint32_t *crc);
int parse_resp_error    (const void *p, uint32_t len, uint16_t *code,
                         char *text, size_t textsz);

int parse_resp_session_id(const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *file_count);
/* Shared by REQ_SESSION_RESUME, RESP_RESUME_ACK and REQ_ACK: all three are
 * session_id(8) + u32(4) + u64(8), so one parser serves them. */
int parse_sid_idx_off    (const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *idx, uint64_t *off);
int parse_resp_checkpoint(const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *done, uint32_t *total);
int parse_sid_only       (const void *p, uint32_t len, uint64_t *sid);
int parse_sid_conn       (const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *conn);
/* Shared by REQ_RANGE, RESP_RANGE_DONE and REQ_RANGE_ACK: all three are
 * session_id(8) + file_idx(4) + u64(8) + u64(8). */
int parse_sid_idx_2off   (const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *idx, uint64_t *a, uint64_t *b);
/* RESP_RANGE_CHUNK: splits the 12-byte prefix from the data that follows.
 * `*data` points into `p`, so it stays valid only as long as `p` does. */
int parse_range_chunk_pfx(const void *p, uint32_t len, uint32_t *idx,
                          uint64_t *abs_offset, const uint8_t **data,
                          uint32_t *data_len);

/* One entry of a RESP_FILE_LIST fragment. */
typedef struct {
    char     name[MAX_NAME_LEN];
    uint64_t size;
    uint32_t crc32;
} file_meta_t;

/* Parse a RESP_FILE_LIST fragment.  Calls `sink` once per entry; `sink` returns
 * 0 to continue or -1 to abort.  Returns 0 on success, -1 on a malformed
 * fragment or if the sink aborted. */
int parse_resp_file_list(const void *p, uint32_t len,
                         int (*sink)(void *ctx, const file_meta_t *m),
                         void *ctx);

#endif /* IT305_PROTOCOL_H */
