#include "common.h"
#include "logging.h"
#include "protocol.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

/* MSG_NOSIGNAL is a Linux flag; macOS and the other BSDs do not define it and
 * offer the per-socket SO_NOSIGPIPE instead.  Falling back to 0 is correct
 * here because every program in this project installs SIG_IGN for SIGPIPE
 * before it opens a socket, so a send() to a vanished peer still returns
 * EPIPE rather than killing the process - which is the behaviour the flag was
 * requesting in the first place.  This is what lets the client build and run
 * on macOS for the three-machine experiment. */
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* ======================= layer 1: byte-stream I/O ======================= */

io_rc io_send_all(int fd, const void *buf, size_t n, uint64_t *wire)
{
    const char *p = (const char *)buf;
    size_t sent = 0;

    while (sent < n) {
        /* MSG_NOSIGNAL: writing to a socket whose peer has gone away must
         * return EPIPE, never raise SIGPIPE and kill the process.  Fault
         * injection makes that a routine event in Part II, not a rare one. */
        ssize_t k = send(fd, p + sent, n - sent, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            if (wire) *wire += sent;
            return IO_ERR;
        }
        if (k == 0)          /* cannot happen for a blocking socket, but be safe */
            break;
        sent += (size_t)k;
    }
    if (wire) *wire += sent;
    return sent == n ? IO_OK : IO_ERR;
}

io_rc io_recv_all(int fd, void *buf, size_t n, uint64_t *wire)
{
    char *p = (char *)buf;
    size_t got = 0;

    while (got < n) {
        ssize_t k = recv(fd, p + got, n - got, 0);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            if (wire) *wire += got;
            return IO_ERR;
        }
        if (k == 0) {        /* orderly shutdown by the peer */
            if (wire) *wire += got;
            /* A clean close exactly on a message boundary is EOF; a close in
             * the middle of one is a truncated message, which is an error. */
            return got == 0 ? IO_EOF : IO_ERR;
        }
        got += (size_t)k;
    }
    if (wire) *wire += got;
    return IO_OK;
}

/* ========================== layer 2: framing =========================== */

static void pack_hdr(uint8_t h[MSG_HEADER_SIZE], uint8_t type, uint8_t flags,
                     uint32_t len)
{
    h[0] = type;
    h[1] = flags;
    h[2] = (uint8_t)(len >> 24);
    h[3] = (uint8_t)(len >> 16);
    h[4] = (uint8_t)(len >> 8);
    h[5] = (uint8_t)(len);
}

io_rc msg_send(int fd, uint8_t type, uint8_t flags,
               const void *payload, uint32_t len, uint64_t *wire)
{
    return msg_send_v(fd, type, flags, payload, len, NULL, 0, wire);
}

io_rc msg_send_v(int fd, uint8_t type, uint8_t flags,
                 const void *p1, uint32_t n1,
                 const void *p2, uint32_t n2, uint64_t *wire)
{
    uint8_t hdr[MSG_HEADER_SIZE];
    pack_hdr(hdr, type, flags, n1 + n2);

    struct iovec iov[3];
    int niov = 0;
    iov[niov].iov_base = hdr;              iov[niov].iov_len = MSG_HEADER_SIZE; niov++;
    if (p1 && n1) { iov[niov].iov_base = (void *)p1; iov[niov].iov_len = n1; niov++; }
    if (p2 && n2) { iov[niov].iov_base = (void *)p2; iov[niov].iov_len = n2; niov++; }

    struct msghdr mh;
    memset(&mh, 0, sizeof mh);
    mh.msg_iov    = iov;
    mh.msg_iovlen = (size_t)niov;

    size_t total = (size_t)MSG_HEADER_SIZE + n1 + n2;
    size_t sent  = 0;

    /* sendmsg() may still send short, so advance through the iovecs by hand
     * rather than assuming one call suffices. */
    while (sent < total) {
        ssize_t k = sendmsg(fd, &mh, MSG_NOSIGNAL);
        if (k < 0) {
            if (errno == EINTR)
                continue;
            if (wire) *wire += sent;
            return IO_ERR;
        }
        sent += (size_t)k;
        if (sent >= total)
            break;

        /* Skip whole iovecs that are now fully sent, then trim the next one. */
        size_t adv = (size_t)k;
        while (adv > 0 && mh.msg_iovlen > 0) {
            if (adv >= mh.msg_iov[0].iov_len) {
                adv -= mh.msg_iov[0].iov_len;
                mh.msg_iov++;
                mh.msg_iovlen--;
            } else {
                mh.msg_iov[0].iov_base = (char *)mh.msg_iov[0].iov_base + adv;
                mh.msg_iov[0].iov_len -= adv;
                adv = 0;
            }
        }
    }
    if (wire) *wire += sent;
    return IO_OK;
}

io_rc msg_recv_hdr(int fd, msg_hdr_t *out, uint64_t *wire)
{
    uint8_t h[MSG_HEADER_SIZE];
    io_rc rc = io_recv_all(fd, h, sizeof h, wire);
    if (rc != IO_OK)
        return rc;

    out->type        = h[0];
    out->flags       = h[1];
    out->payload_len = ((uint32_t)h[2] << 24) | ((uint32_t)h[3] << 16) |
                       ((uint32_t)h[4] << 8)  |  (uint32_t)h[5];

    if (out->payload_len > MAX_PAYLOAD_LEN) {
        log_error("protocol: declared payload_len %u exceeds limit %u (type 0x%02x)",
                  out->payload_len, MAX_PAYLOAD_LEN, out->type);
        errno = EPROTO;
        return IO_ERR;
    }
    return IO_OK;
}

io_rc msg_recv_payload(int fd, void *buf, uint32_t len, uint64_t *wire)
{
    if (len == 0)
        return IO_OK;
    io_rc rc = io_recv_all(fd, buf, len, wire);
    /* A clean close part-way through a payload is a truncated message. */
    return rc == IO_EOF ? IO_ERR : rc;
}

io_rc msg_skip_payload(int fd, uint32_t len, uint64_t *wire)
{
    char sink[4096];
    while (len > 0) {
        uint32_t take = len < sizeof sink ? len : (uint32_t)sizeof sink;
        io_rc rc = msg_recv_payload(fd, sink, take, wire);
        if (rc != IO_OK)
            return rc;
        len -= take;
    }
    return IO_OK;
}

const char *msg_type_name(uint8_t type)
{
    switch (type) {
    case MSG_REQ_TOPIC:          return "REQ_TOPIC";
    case MSG_RESP_META:          return "RESP_META";
    case MSG_RESP_FILE_INFO:     return "RESP_FILE_INFO";
    case MSG_RESP_FILE_CHUNK:    return "RESP_FILE_CHUNK";
    case MSG_RESP_DONE:          return "RESP_DONE";
    case MSG_RESP_ERROR:         return "RESP_ERROR";
    case MSG_REQ_SESSION_START:  return "REQ_SESSION_START";
    case MSG_RESP_SESSION_ID:    return "RESP_SESSION_ID";
    case MSG_REQ_SESSION_RESUME: return "REQ_SESSION_RESUME";
    case MSG_RESP_RESUME_ACK:    return "RESP_RESUME_ACK";
    case MSG_RESP_CHECKPOINT:    return "RESP_CHECKPOINT";
    case MSG_REQ_ACK:            return "REQ_ACK";
    case MSG_REQ_RANGE:          return "REQ_RANGE";
    case MSG_RESP_RANGE_ACK:     return "RESP_RANGE_ACK";
    case MSG_REQ_FILE_LIST:      return "REQ_FILE_LIST";
    case MSG_RESP_FILE_LIST:     return "RESP_FILE_LIST";
    case MSG_REQ_ATTACH:         return "REQ_ATTACH";
    case MSG_RESP_ATTACH_ACK:    return "RESP_ATTACH_ACK";
    case MSG_RESP_RANGE_CHUNK:   return "RESP_RANGE_CHUNK";
    case MSG_RESP_RANGE_DONE:    return "RESP_RANGE_DONE";
    case MSG_REQ_RANGE_ACK:      return "REQ_RANGE_ACK";
    default:                     return "UNKNOWN";
    }
}

const char *err_code_name(uint16_t code)
{
    switch (code) {
    case ERR_UNKNOWN_TOPIC: return "ERR_UNKNOWN_TOPIC";
    case ERR_BAD_REQUEST:   return "ERR_BAD_REQUEST";
    case ERR_INVALID_NAME:  return "ERR_INVALID_NAME";
    case ERR_IO:            return "ERR_IO";
    case ERR_NO_SESSION:    return "ERR_NO_SESSION";
    case ERR_BAD_RANGE:     return "ERR_BAD_RANGE";
    case ERR_INTERNAL:      return "ERR_INTERNAL";
    default:                return "ERR_UNKNOWN";
    }
}

/* ===================== payload packing and parsing ===================== */

void pb_init(pb_t *b, void *buf, size_t cap)
{
    b->buf = (uint8_t *)buf;
    b->cap = cap;
    b->len = 0;
    b->overflow = 0;
}

/* Every pb_* putter funnels through here, so the bounds check exists once. */
static uint8_t *pb_claim(pb_t *b, size_t n)
{
    if (b->overflow || b->len + n > b->cap) {
        b->overflow = 1;
        return NULL;
    }
    uint8_t *at = b->buf + b->len;
    b->len += n;
    return at;
}

void pb_u8(pb_t *b, uint8_t v)
{
    uint8_t *p = pb_claim(b, 1);
    if (p) p[0] = v;
}

void pb_u16(pb_t *b, uint16_t v)
{
    uint8_t *p = pb_claim(b, 2);
    if (p) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
}

void pb_u32(pb_t *b, uint32_t v)
{
    uint8_t *p = pb_claim(b, 4);
    if (p) {
        p[0] = (uint8_t)(v >> 24); p[1] = (uint8_t)(v >> 16);
        p[2] = (uint8_t)(v >> 8);  p[3] = (uint8_t)v;
    }
}

void pb_u64(pb_t *b, uint64_t v)
{
    uint8_t *p = pb_claim(b, 8);
    if (p)
        for (int i = 0; i < 8; i++)
            p[i] = (uint8_t)(v >> (56 - 8 * i));
}

void pb_bytes(pb_t *b, const void *p, size_t n)
{
    uint8_t *at = pb_claim(b, n);
    if (at && n) memcpy(at, p, n);
}

int pb_ok(const pb_t *b) { return b->overflow ? -1 : 0; }

void pv_init(pv_t *v, const void *buf, size_t len)
{
    v->buf = (const uint8_t *)buf;
    v->len = len;
    v->off = 0;
    v->bad = 0;
}

/* Every pv_* getter funnels through here, so the underflow check exists once.
 * On underflow the flag is set and NULL returned; getters then yield 0 and the
 * caller discovers the problem in one place, at pv_done(). */
static const uint8_t *pv_take(pv_t *v, size_t n)
{
    if (v->bad || v->off + n > v->len) {
        v->bad = 1;
        return NULL;
    }
    const uint8_t *at = v->buf + v->off;
    v->off += n;
    return at;
}

uint8_t pv_u8(pv_t *v)
{
    const uint8_t *p = pv_take(v, 1);
    return p ? p[0] : 0;
}

uint16_t pv_u16(pv_t *v)
{
    const uint8_t *p = pv_take(v, 2);
    return p ? (uint16_t)(((uint16_t)p[0] << 8) | p[1]) : 0;
}

uint32_t pv_u32(pv_t *v)
{
    const uint8_t *p = pv_take(v, 4);
    if (!p) return 0;
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) |
           ((uint32_t)p[2] << 8)  |  (uint32_t)p[3];
}

uint64_t pv_u64(pv_t *v)
{
    const uint8_t *p = pv_take(v, 8);
    if (!p) return 0;
    uint64_t r = 0;
    for (int i = 0; i < 8; i++)
        r = (r << 8) | p[i];
    return r;
}

const uint8_t *pv_bytes(pv_t *v, size_t n) { return pv_take(v, n); }

int pv_done(const pv_t *v)
{
    return (v->bad || v->off != v->len) ? -1 : 0;
}

/* ============================ name safety ============================== */

/* Shared by both name checks: non-empty, fits the limit, no '/', and not a
 * directory-traversal component.  Rejecting '/' outright is what makes "the
 * result always stays inside the directory we joined it onto" true by
 * construction - there is no need to canonicalise the path afterwards. */
static int name_safe(const char *s, size_t limit)
{
    if (!s || !*s)
        return -1;
    size_t n = strlen(s);
    if (n >= limit)
        return -1;
    if (strchr(s, '/'))
        return -1;
    if (strcmp(s, ".") == 0 || strcmp(s, "..") == 0)
        return -1;
    /* Belt and braces: ".." cannot appear as a whole component once '/' is
     * banned, but reject an embedded ".." too so the rule reads the same way
     * as it is stated in PROTOCOL.md. */
    if (strstr(s, ".."))
        return -1;
    return 0;
}

int name_safe_topic(const char *s) { return name_safe(s, MAX_KEY_LEN); }
int name_safe_file (const char *s) { return name_safe(s, MAX_NAME_LEN); }

/* ========================= layer 3: senders ============================ */

/* Most payloads are small and fixed-shape, so they are packed into a stack
 * buffer and sent in one msg_send().  Only the two chunk types take a separate
 * data pointer, and they use msg_send_v() to avoid copying the file bytes. */

static io_rc send_sid_u32_u64(int fd, uint8_t type, uint64_t sid, uint32_t a,
                              uint64_t b, uint64_t *wire)
{
    uint8_t p[20];
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u64(&w, sid);
    pb_u32(&w, a);
    pb_u64(&w, b);
    if (pb_ok(&w) < 0) { errno = EOVERFLOW; return IO_ERR; }
    return msg_send(fd, type, FLAG_NONE, p, (uint32_t)w.len, wire);
}

static io_rc send_sid_u32_2u64(int fd, uint8_t type, uint64_t sid, uint32_t idx,
                               uint64_t a, uint64_t b, uint64_t *wire)
{
    uint8_t p[28];
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u64(&w, sid);
    pb_u32(&w, idx);
    pb_u64(&w, a);
    pb_u64(&w, b);
    if (pb_ok(&w) < 0) { errno = EOVERFLOW; return IO_ERR; }
    return msg_send(fd, type, FLAG_NONE, p, (uint32_t)w.len, wire);
}

static io_rc send_topic_msg(int fd, uint8_t type, const char *topic, uint64_t *wire)
{
    uint8_t p[2 + MAX_KEY_LEN];
    size_t n = strlen(topic);
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u16(&w, (uint16_t)n);
    pb_bytes(&w, topic, n);
    if (pb_ok(&w) < 0) { errno = EOVERFLOW; return IO_ERR; }
    return msg_send(fd, type, FLAG_NONE, p, (uint32_t)w.len, wire);
}

io_rc send_req_topic(int fd, const char *topic, uint64_t *wire)
{
    return send_topic_msg(fd, MSG_REQ_TOPIC, topic, wire);
}

io_rc send_req_session_start(int fd, const char *topic, uint64_t *wire)
{
    return send_topic_msg(fd, MSG_REQ_SESSION_START, topic, wire);
}

io_rc send_resp_meta(int fd, uint32_t file_count, uint64_t *wire)
{
    uint8_t p[4];
    pb_t w; pb_init(&w, p, sizeof p); pb_u32(&w, file_count);
    return msg_send(fd, MSG_RESP_META, FLAG_NONE, p, (uint32_t)w.len, wire);
}

io_rc send_resp_file_info(int fd, uint8_t flags, const char *name,
                          uint64_t size, uint32_t crc, uint64_t *wire)
{
    uint8_t p[2 + MAX_NAME_LEN + 8 + 4];
    size_t n = strlen(name);
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u16(&w, (uint16_t)n);
    pb_bytes(&w, name, n);
    pb_u64(&w, size);
    pb_u32(&w, crc);
    if (pb_ok(&w) < 0) { errno = EOVERFLOW; return IO_ERR; }
    return msg_send(fd, MSG_RESP_FILE_INFO, flags, p, (uint32_t)w.len, wire);
}

io_rc send_resp_done(int fd, uint64_t *wire)
{
    return msg_send(fd, MSG_RESP_DONE, FLAG_NONE, NULL, 0, wire);
}

io_rc send_resp_error(int fd, uint16_t code, const char *fmt, ...)
{
    char text[256];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(text, sizeof text, fmt, ap);
    va_end(ap);

    uint8_t p[4 + sizeof text];
    size_t n = strlen(text);
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u16(&w, code);
    pb_u16(&w, (uint16_t)n);
    pb_bytes(&w, text, n);
    if (pb_ok(&w) < 0) { errno = EOVERFLOW; return IO_ERR; }
    /* No wire counter: an error reply is control traffic on a dying
     * connection, and is accounted for in the server log rather than in the
     * client's throughput numbers. */
    return msg_send(fd, MSG_RESP_ERROR, FLAG_NONE, p, (uint32_t)w.len, NULL);
}

io_rc send_resp_session_id(int fd, uint64_t sid, uint32_t file_count, uint64_t *wire)
{
    uint8_t p[12];
    pb_t w; pb_init(&w, p, sizeof p); pb_u64(&w, sid); pb_u32(&w, file_count);
    return msg_send(fd, MSG_RESP_SESSION_ID, FLAG_NONE, p, (uint32_t)w.len, wire);
}

io_rc send_req_session_resume(int fd, uint64_t sid, uint32_t idx, uint64_t off,
                              uint64_t *wire)
{
    return send_sid_u32_u64(fd, MSG_REQ_SESSION_RESUME, sid, idx, off, wire);
}

io_rc send_resp_resume_ack(int fd, uint64_t sid, uint32_t idx, uint64_t off,
                           uint64_t *wire)
{
    return send_sid_u32_u64(fd, MSG_RESP_RESUME_ACK, sid, idx, off, wire);
}

io_rc send_req_ack(int fd, uint64_t sid, uint32_t idx, uint64_t off, uint64_t *wire)
{
    return send_sid_u32_u64(fd, MSG_REQ_ACK, sid, idx, off, wire);
}

io_rc send_resp_checkpoint(int fd, uint64_t sid, uint32_t done, uint32_t total,
                           uint64_t *wire)
{
    uint8_t p[16];
    pb_t w;
    pb_init(&w, p, sizeof p);
    pb_u64(&w, sid);
    pb_u32(&w, done);
    pb_u32(&w, total);
    return msg_send(fd, MSG_RESP_CHECKPOINT, FLAG_NONE, p, (uint32_t)w.len, wire);
}

io_rc send_req_range(int fd, uint64_t sid, uint32_t idx, uint64_t start,
                     uint64_t end, uint64_t *wire)
{
    return send_sid_u32_2u64(fd, MSG_REQ_RANGE, sid, idx, start, end, wire);
}

io_rc send_resp_range_ack(int fd, uint64_t sid, uint32_t idx, uint64_t start,
                          uint64_t *wire)
{
    return send_sid_u32_u64(fd, MSG_RESP_RANGE_ACK, sid, idx, start, wire);
}

io_rc send_resp_range_done(int fd, uint64_t sid, uint32_t idx, uint64_t start,
                           uint64_t end, uint64_t *wire)
{
    return send_sid_u32_2u64(fd, MSG_RESP_RANGE_DONE, sid, idx, start, end, wire);
}

io_rc send_req_range_ack(int fd, uint64_t sid, uint32_t idx, uint64_t range_start,
                         uint64_t abs_offset, uint64_t *wire)
{
    return send_sid_u32_2u64(fd, MSG_REQ_RANGE_ACK, sid, idx, range_start,
                             abs_offset, wire);
}

io_rc send_req_file_list(int fd, uint64_t sid, uint64_t *wire)
{
    uint8_t p[8];
    pb_t w; pb_init(&w, p, sizeof p); pb_u64(&w, sid);
    return msg_send(fd, MSG_REQ_FILE_LIST, FLAG_NONE, p, (uint32_t)w.len, wire);
}

static io_rc send_sid_conn(int fd, uint8_t type, uint64_t sid, uint32_t conn,
                           uint64_t *wire)
{
    uint8_t p[12];
    pb_t w; pb_init(&w, p, sizeof p); pb_u64(&w, sid); pb_u32(&w, conn);
    return msg_send(fd, type, FLAG_NONE, p, (uint32_t)w.len, wire);
}

io_rc send_req_attach(int fd, uint64_t sid, uint32_t conn, uint64_t *wire)
{
    return send_sid_conn(fd, MSG_REQ_ATTACH, sid, conn, wire);
}

io_rc send_resp_attach_ack(int fd, uint64_t sid, uint32_t conn, uint64_t *wire)
{
    return send_sid_conn(fd, MSG_RESP_ATTACH_ACK, sid, conn, wire);
}

/* ========================= layer 3: parsers ============================ */

int parse_topic_name(const void *p, uint32_t len, char *out, size_t outsz)
{
    pv_t v;
    pv_init(&v, p, len);
    uint16_t n = pv_u16(&v);
    const uint8_t *s = pv_bytes(&v, n);
    if (pv_done(&v) < 0 || !s || n == 0 || (size_t)n >= outsz)
        return -1;
    memcpy(out, s, n);
    out[n] = '\0';
    return 0;
}

int parse_u32_only(const void *p, uint32_t len, uint32_t *val)
{
    pv_t v;
    pv_init(&v, p, len);
    uint32_t x = pv_u32(&v);
    if (pv_done(&v) < 0)
        return -1;
    *val = x;
    return 0;
}

int parse_resp_file_info(const void *p, uint32_t len, char *name, size_t namesz,
                         uint64_t *size, uint32_t *crc)
{
    pv_t v;
    pv_init(&v, p, len);
    uint16_t n = pv_u16(&v);
    const uint8_t *s = pv_bytes(&v, n);
    uint64_t sz = pv_u64(&v);
    uint32_t c  = pv_u32(&v);
    if (pv_done(&v) < 0 || !s || n == 0 || (size_t)n >= namesz)
        return -1;
    memcpy(name, s, n);
    name[n] = '\0';
    *size = sz;
    *crc  = c;
    return 0;
}

int parse_resp_error(const void *p, uint32_t len, uint16_t *code, char *text,
                     size_t textsz)
{
    pv_t v;
    pv_init(&v, p, len);
    uint16_t c = pv_u16(&v);
    uint16_t n = pv_u16(&v);
    const uint8_t *s = pv_bytes(&v, n);
    if (pv_done(&v) < 0 || !s)
        return -1;
    size_t take = (size_t)n < textsz - 1 ? (size_t)n : textsz - 1;
    memcpy(text, s, take);
    text[take] = '\0';
    *code = c;
    return 0;
}

int parse_resp_session_id(const void *p, uint32_t len, uint64_t *sid, uint32_t *n)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s = pv_u64(&v);
    uint32_t c = pv_u32(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s;
    *n   = c;
    return 0;
}

int parse_sid_idx_off(const void *p, uint32_t len, uint64_t *sid, uint32_t *idx,
                      uint64_t *off)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s = pv_u64(&v);
    uint32_t i = pv_u32(&v);
    uint64_t o = pv_u64(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s; *idx = i; *off = o;
    return 0;
}

int parse_resp_checkpoint(const void *p, uint32_t len, uint64_t *sid,
                          uint32_t *done, uint32_t *total)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s = pv_u64(&v);
    uint32_t d = pv_u32(&v);
    uint32_t t = pv_u32(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s; *done = d; *total = t;
    return 0;
}

int parse_sid_only(const void *p, uint32_t len, uint64_t *sid)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s = pv_u64(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s;
    return 0;
}

int parse_sid_conn(const void *p, uint32_t len, uint64_t *sid, uint32_t *conn)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s = pv_u64(&v);
    uint32_t c = pv_u32(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s; *conn = c;
    return 0;
}

int parse_sid_idx_2off(const void *p, uint32_t len, uint64_t *sid, uint32_t *idx,
                       uint64_t *a, uint64_t *b)
{
    pv_t v;
    pv_init(&v, p, len);
    uint64_t s  = pv_u64(&v);
    uint32_t i  = pv_u32(&v);
    uint64_t x  = pv_u64(&v);
    uint64_t y  = pv_u64(&v);
    if (pv_done(&v) < 0)
        return -1;
    *sid = s; *idx = i; *a = x; *b = y;
    return 0;
}

int parse_range_chunk_pfx(const void *p, uint32_t len, uint32_t *idx,
                          uint64_t *abs_offset, const uint8_t **data,
                          uint32_t *data_len)
{
    if (len < SZ_RANGE_CHUNK_PFX)
        return -1;
    pv_t v;
    pv_init(&v, p, SZ_RANGE_CHUNK_PFX);   /* parse only the prefix */
    uint32_t i = pv_u32(&v);
    uint64_t o = pv_u64(&v);
    if (pv_done(&v) < 0)
        return -1;
    *idx        = i;
    *abs_offset = o;
    *data       = (const uint8_t *)p + SZ_RANGE_CHUNK_PFX;
    *data_len   = len - SZ_RANGE_CHUNK_PFX;
    return 0;
}

int parse_resp_file_list(const void *p, uint32_t len,
                         int (*sink)(void *ctx, const file_meta_t *m), void *ctx)
{
    pv_t v;
    pv_init(&v, p, len);
    uint32_t count = pv_u32(&v);

    for (uint32_t k = 0; k < count; k++) {
        file_meta_t m;
        uint16_t n = pv_u16(&v);
        const uint8_t *s = pv_bytes(&v, n);
        uint64_t sz = pv_u64(&v);
        uint32_t c  = pv_u32(&v);
        if (v.bad || !s || n == 0 || (size_t)n >= sizeof m.name)
            return -1;
        memcpy(m.name, s, n);
        m.name[n] = '\0';
        m.size  = sz;
        m.crc32 = c;
        if (sink(ctx, &m) < 0)
            return -1;
    }
    return pv_done(&v);
}

io_rc msg_try_recv_hdr(int fd, msg_hdr_t *out, uint64_t *wire)
{
    uint8_t h[MSG_HEADER_SIZE];
    ssize_t first;

    /* One non-blocking probe.  EAGAIN with nothing read means "no message
     * pending", which is the common case and the whole point of this call. */
    for (;;) {
        first = recv(fd, h, sizeof h, MSG_DONTWAIT);
        if (first >= 0)
            break;
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return IO_AGAIN;
        return IO_ERR;
    }
    if (first == 0)
        return IO_EOF;

    if (wire) *wire += (size_t)first;

    /* A header can arrive split across segments.  The rest is already on its
     * way, so finishing the read with a blocking call is safe and bounded. */
    if ((size_t)first < sizeof h) {
        io_rc rc = io_recv_all(fd, h + first, sizeof h - (size_t)first, wire);
        if (rc != IO_OK)
            return rc == IO_EOF ? IO_ERR : rc;   /* a split header must complete */
    }

    out->type        = h[0];
    out->flags       = h[1];
    out->payload_len = ((uint32_t)h[2] << 24) | ((uint32_t)h[3] << 16) |
                       ((uint32_t)h[4] << 8)  |  (uint32_t)h[5];

    if (out->payload_len > MAX_PAYLOAD_LEN) {
        log_error("protocol: declared payload_len %u exceeds limit %u (type 0x%02x)",
                  out->payload_len, MAX_PAYLOAD_LEN, out->type);
        errno = EPROTO;
        return IO_ERR;
    }
    return IO_OK;
}
