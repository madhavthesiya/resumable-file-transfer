# PROTOCOL.md - Wire Protocol Specification

**Fault-Tolerant Topic-Based File Distribution Service**
Originally written for IT305 (Computer Networks), lab groups 88 & 92.

This file is the **single source of truth** for the wire format. Every message
type, field layout and message flow used by `baseline/`, `restart/`,
`resume/` and `parallel/` is defined here. The implementation in
`src/common/protocol.{h,c}` mirrors this document exactly; if the two ever disagree,
this document is the specification and the code is the bug.

---

## 1. Transport and byte order

* IPv4 TCP stream sockets (`AF_INET`, `SOCK_STREAM`).
* All multi-byte integer fields are **big-endian** (network byte order).
* There are no alignment or padding bytes anywhere: every message is a packed
  byte sequence exactly as tabulated. Structs are never `memcpy`'d onto the wire
  - each field is packed explicitly - so the format does not depend on the
  compiler's struct layout.
* Every numeric field is unsigned.
* A connection carries one *or more* messages back to back, with no delimiter
  other than the length field in each header.

## 2. Common header

Every message in every part and case begins with the same 6-byte header.

| Offset | Size | Field         | Description                                 |
|--------|------|---------------|---------------------------------------------|
| 0      | 1    | `msg_type`    | Message type (Sections 4-6)                 |
| 1      | 1    | `flags`       | Bit flags (Section 3)                       |
| 2      | 4    | `payload_len` | Length in bytes of the payload that follows |

Total header size: **6 bytes** (`MSG_HEADER_SIZE`).

`payload_len` may be 0. A receiver **must** reject `payload_len > MAX_PAYLOAD_LEN`
(16 MiB) as a protocol error and close the connection rather than attempting the
allocation - this bounds memory use even if a peer is malfunctioning or hostile.

## 3. Flags

| Bit  | Name           | Meaning                                                  |
|------|----------------|----------------------------------------------------------|
| 0x01 | `FLAG_MORE`    | Continuation fragment; more of the same logical sequence follows. Used only by `RESP_FILE_LIST`. |
| 0x02 | `FLAG_RESUMED` | Informational: set on the `RESP_FILE_INFO` of a file whose transfer is resuming part-way. Diagnostics only. |

All other bits are reserved and sent as 0. A receiver ignores unknown flag bits
(forward compatibility) but never ignores an unknown `msg_type`.

## 4. Base message set (used unchanged by `restart`)

| Value | Name              | Direction | Payload                                                      |
|-------|-------------------|-----------|--------------------------------------------------------------|
| 0x01  | `REQ_TOPIC`       | C to S    | `topic_len`(2) + `topic`(topic_len bytes, UTF-8, not NUL-terminated) |
| 0x02  | `RESP_META`       | S to C    | `file_count`(4)                                              |
| 0x03  | `RESP_FILE_INFO`  | S to C    | `name_len`(2) + `name`(name_len) + `file_size`(8) + `crc32`(4) |
| 0x04  | `RESP_FILE_CHUNK` | S to C    | raw file bytes, 1..`CHUNK_SIZE` (default 16384)              |
| 0x05  | `RESP_DONE`       | S to C    | *(empty)* - topic transfer complete (in Enhanced: this connection has no more work) |
| 0x06  | `RESP_ERROR`      | S to C    | `err_code`(2) + `msg_len`(2) + `msg`(UTF-8 human-readable)    |

### 4.1 Error codes carried in `RESP_ERROR`

| Code | Name                | Meaning                                                   |
|------|---------------------|-----------------------------------------------------------|
| 1    | `ERR_UNKNOWN_TOPIC` | Topic not present in the server's topic table             |
| 2    | `ERR_BAD_REQUEST`   | Malformed message, or illegal for the current protocol state |
| 3    | `ERR_INVALID_NAME`  | Name rejected by the path-safety check (`..`, leading `/`, or any `/` in a file name) |
| 4    | `ERR_IO`            | Server-side filesystem error while reading a file         |
| 5    | `ERR_NO_SESSION`    | `session_id` unknown, expired or evicted                  |
| 6    | `ERR_BAD_RANGE`     | Range outside `[0, file_size]`, or inverted               |
| 7    | `ERR_INTERNAL`      | Server resource exhaustion (e.g. session table full)      |

`RESP_ERROR` is always terminal: the server closes the socket right after sending it.

### 4.2 Base message flow

```
Client                                   Server
  |-- REQ_TOPIC("gallina") ------------->|  look topic up in the table
  |                                      |  (absent -> RESP_ERROR, close)
  |<------------ RESP_META(3098) --------|
  |                                      |
  |      for each file i, in cached sorted order:
  |<----- RESP_FILE_INFO(name,size,crc32)|
  |<----- RESP_FILE_CHUNK x ceil(size/16384)
  |                                      |
  |<------------ RESP_DONE --------------|
  |  verify CRC32 of every file; close   |
```

A zero-length file is announced with `file_size = 0` and followed by **zero**
`RESP_FILE_CHUNK` messages - the client must not wait for data. (The synthetic
test set contains such a file precisely to exercise this path.)

`RESP_FILE_CHUNK` carries no offset field: within one file, chunks are strictly
sequential, so the receiver's running offset is implicit. That holds per-chunk
overhead to 6 bytes per 16 KiB (0.037 %).

## 5. Session-management additions (`resume`)

`resume` uses the whole base set plus the following. The 8-byte `session_id` is
generated by the server and echoed by the client on every subsequent message.

| Value | Name                 | Direction | Payload                                            |
|-------|----------------------|-----------|----------------------------------------------------|
| 0x10  | `REQ_SESSION_START`  | C to S    | `topic_len`(2) + `topic`                           |
| 0x11  | `RESP_SESSION_ID`    | S to C    | `session_id`(8) + `file_count`(4)                  |
| 0x12  | `REQ_SESSION_RESUME` | C to S    | `session_id`(8) + `file_idx`(4) + `byte_offset`(8) |
| 0x13  | `RESP_RESUME_ACK`    | S to C    | `session_id`(8) + `start_file_idx`(4) + `start_offset`(8) |
| 0x14  | `RESP_CHECKPOINT`    | S to C    | `session_id`(8) + `completed_files`(4) + `total_files`(4) |
| 0x15  | `REQ_ACK`            | C to S    | `session_id`(8) + `file_idx`(4) + `byte_offset`(8) |

`RESP_RESUME_ACK` with `start_file_idx == 0xFFFFFFFF` means **resume rejected**:
the client discards its checkpoint and restarts with `REQ_SESSION_START`.

### 5.1 Resume flow - fresh transfer

```
Client                                        Server
  |-- REQ_SESSION_START("gallina") ---------->|  create session, insert in table
  |<-- RESP_SESSION_ID(id, file_count) -------|
  |<-- RESP_META(file_count) -----------------|
  |      per file: RESP_FILE_INFO + chunks    |
  |-- REQ_ACK(id, file_idx, offset) --------->|  every CHECKPOINT_INTERVAL (64 KiB)
  |                                           |  server: bytes_acked = offset
  |<-- RESP_CHECKPOINT(id, done, total) ------|  at each file boundary
  |<-- RESP_DONE -----------------------------|  session destroyed
```

### 5.2 Resume flow - fault mid-file, then resume

```
  ... streaming file 46; server has bytes_acked = 81920 ...
  [fault injected] -> server closes the socket

Client sees EOF/ECONNRESET and keeps its on-disk checkpoint (file 46, 94208)
  |== new TCP connection ==                   |
  |-- REQ_SESSION_RESUME(id, 46, 94208) ----->|  validate (Section 5.2.2)
  |                                           |  resume_off = min(94208, 81920)
  |<-- RESP_RESUME_ACK(id, 46, 81920) --------|
  |<-- RESP_FILE_INFO(file 46) [FLAG_RESUMED] |  file_size is the FULL size
  |      client lseek()s its output to 81920  |
  |<-- RESP_FILE_CHUNK from offset 81920 .....|  12288 bytes re-sent = redundant
  |      ... continues with files 47..N ...   |
  |<-- RESP_DONE -----------------------------|
```

`RESP_FILE_INFO` always announces the **full** file size, in every part and case.
The resume offset is carried solely by `RESP_RESUME_ACK`, so the base message
layout is reused byte for byte and the client needs no special parsing mode.
Only the first file after a resume starts at a non-zero offset; later files
start at 0.

#### 5.2.1 What a position means

A position is a `(file_idx, byte_offset)` pair, and it means exactly:

> every file with an index below `file_idx` is complete on the client's disk,
> and the first `byte_offset` bytes of file `file_idx` are on the client's disk.

So a client that has just finished file 5 advances to `(6, 0)`, **not**
`(5, size_of_file_5)`. That rule is what keeps a zero-length file
unambiguous: `(5, 0)` always means "file 5 still to come", whatever its size,
whereas `(5, size)` with `size == 0` would be indistinguishable from it.

#### 5.2.2 Resume validation, in order

1. `session_id` exists in the session table and has not been evicted for idleness.
2. `file_idx <= file_count` (equal means the transfer is already complete, so
   the server acknowledges and sends `RESP_DONE` straight away).
3. `byte_offset <= size(file[file_idx])`.
4. `resume_off = min(client_position, server_sent_position)` under the
   lexicographic order on `(file_idx, byte_offset)`, where
   `server_sent_position` is the high-water mark of what this session has
   actually transmitted. The server will therefore never be talked into
   skipping data it never sent, but it does honour progress the client has
   durably written.

   Honouring the client's claim is safe because the client writes its
   checkpoint to disk **before** sending the matching `REQ_ACK`, so a claim is
   always backed by durable client state; and because the client re-reads those
   bytes to rebuild its CRC (section 5.2.3), a claim that its own disk cannot
   support fails immediately and falls back to a fresh session.

   The design document originally specified clamping to the *acknowledged*
   position instead. That is measurably worse and was changed: the server's
   sends land in the socket buffer and can run megabytes ahead of the ACK
   stream coming back, so at fault time the acknowledged position trails the
   client's real progress by roughly the socket buffer size, not by one
   checkpoint interval. Clamping to it re-sent files the client already had.
   README, "Deviations from the design document", records the measured effect.
   The acknowledged position is still tracked, and is reported, because the gap
   between it and the sent position is exactly the ACK lag the analysis
   discusses.

If check 1, 2 or 3 fails the server replies `start_file_idx = 0xFFFFFFFF`. The
bias is deliberate - **restart rather than guess**: wrongly accepting a resume
would silently leave a hole in the client's file, which is worse than a slower
but correct restart.

There is deliberately **no** "skip a file that looks already complete" step. The
server always re-announces file `file_idx` with a `RESP_FILE_INFO` before any of
its data, even when `resume_off` equals that file's size and no chunks follow.
Without that announcement a client that had not yet created the file would never
learn of it, and a zero-length file at the resume point would be lost.

#### 5.2.3 Rebuilding the CRC after a resume

CRC-32 is computed over a file sequentially, so a client resuming file `f` at
offset `o` has no running CRC state for the first `o` bytes - they were written
by an earlier process run. Before writing any new data the client therefore
re-reads those `o` bytes from its own output file and folds them into a fresh
running CRC, then seeks to `o` and continues. The cost is bounded by one file,
and it keeps the per-file integrity check exact across any number of resumes.
A client whose output file turns out to be shorter than `o` treats its
checkpoint as untrustworthy and falls back to a fresh session.

### 5.3 What each side stores

| State              | Server | Client | Notes                                           |
|--------------------|--------|--------|-------------------------------------------------|
| `session_id`       | yes    | yes    | server-generated, 8 random bytes                |
| topic + file list  | yes    | no     | cached sorted listing; fixes the meaning of `file_idx` |
| `current_file_idx` | yes    | yes    |                                                 |
| `ack_*` position   | yes    | no     | advanced only by `REQ_ACK`; reported, not used for the clamp |
| `sent_*` position  | yes    | no     | high-water mark of what was transmitted; this is what the resume clamp uses |
| `bytes_received`   | no     | yes    | the client's own durable on-disk progress       |
| `last_activity`    | yes    | no     | drives 60 s idle eviction                       |

The client checkpoint is persisted to `output_dir/topic/.checkpoint`, written as
`.checkpoint.tmp` then `rename()`d into place, so a crash can never leave a torn
record. The server keeps session state **in memory only**; if the server process
restarts, resumes against it are rejected and clients fall back to a fresh
session - correct, just slower.

## 6. Parallel-connection additions (`parallel`)

| Value | Name               | Direction | Payload                                                       |
|-------|--------------------|-----------|---------------------------------------------------------------|
| 0x20  | `REQ_RANGE`        | C to S    | `session_id`(8) + `file_idx`(4) + `range_start`(8) + `range_end`(8, exclusive) |
| 0x21  | `RESP_RANGE_ACK`   | S to C    | `session_id`(8) + `file_idx`(4) + `range_start`(8) - the offset the server will actually start from, after the Section 5.2.2 clamp |
| 0x22  | `REQ_FILE_LIST`    | C to S    | `session_id`(8)                                               |
| 0x23  | `RESP_FILE_LIST`   | S to C    | `entry_count`(4) + entry_count x { `name_len`(2) + `name` + `file_size`(8) + `crc32`(4) }; `FLAG_MORE` set on every fragment but the last |
| 0x24  | `REQ_ATTACH`       | C to S    | `session_id`(8) + `conn_index`(4)                             |
| 0x25  | `RESP_ATTACH_ACK`  | S to C    | `session_id`(8) + `conn_index`(4)                             |
| 0x26  | `RESP_RANGE_CHUNK` | S to C    | `file_idx`(4) + `abs_offset`(8) + raw file bytes               |
| 0x27  | `RESP_RANGE_DONE`  | S to C    | `session_id`(8) + `file_idx`(4) + `range_start`(8) + `range_end`(8) |
| 0x28  | `REQ_RANGE_ACK`    | C to S    | `session_id`(8) + `file_idx`(4) + `range_start`(8) + `abs_offset`(8) |

### 6.1 Why the Enhanced case needs its own data message

`RESP_RANGE_CHUNK` carries an explicit 12-byte `(file_idx, abs_offset)` prefix,
unlike `RESP_FILE_CHUNK`. With K connections each streaming a different part of
the topic, the receiver must know where to `pwrite()` without depending on any
shared cursor, and a resume must be able to restate an absolute position. The
cost is 18 bytes per 16 KiB chunk (0.11 %) instead of 6 (0.037 %); that cost is
measured and reported rather than assumed.

### 6.2 Work partitioning

(This is a deliberate deviation from the design document - see README, section
"Deviations from the design document".)

The client fetches the whole file list once (`REQ_FILE_LIST`), then builds an
ordered list of **work items**, each a `(file_idx, range_start, range_end)`
triple, and assigns each to one of K connections:

* `file_size < SPLIT_THRESHOLD` (default 512 KiB): one work item covering the
  whole file, assigned to connection `file_idx mod K`.
* `file_size >= SPLIT_THRESHOLD`: split into K contiguous ranges; range *j* goes
  to connection *j*.

Every transfer in the Enhanced case is therefore a range request - there is no
separate whole-file path on the wire - which keeps the server's Enhanced state
machine down to a single case.

### 6.3 Enhanced flow

```
conn 0:      REQ_SESSION_START(topic) -> RESP_SESSION_ID(id, n)
conn 0:      REQ_FILE_LIST(id)        -> RESP_FILE_LIST (1+ fragments, FLAG_MORE)
conn 1..K-1: REQ_ATTACH(id, j)        -> RESP_ATTACH_ACK(id, j)
             (the listener hands these fds to the session's existing owner
              thread; no new server thread is created - see README, "Threading")

each conn j, for each of its work items in order:
    REQ_RANGE(id, file_idx, start, end)
      -> RESP_RANGE_ACK(id, file_idx, actual_start)
      -> RESP_RANGE_CHUNK ...        (client pwrite()s at abs_offset)
      -> RESP_RANGE_DONE(id, file_idx, start, end)
    REQ_RANGE_ACK(id, file_idx, start, abs_offset)   every 64 KiB

when a connection's work list is exhausted: RESP_DONE on that connection
```

On reconnect after a fault the affected connection re-issues `REQ_ATTACH` (or
`REQ_SESSION_RESUME` if it is connection 0) and then `REQ_RANGE` for the
remaining part of its current work item only. The other K-1 connections are
unaffected and keep streaming.

### 6.4 Per-connection progress record (server side)

```c
typedef struct {
    uint32_t file_idx;
    uint64_t range_start;    /* identifies the work item         */
    uint64_t range_end;
    uint64_t bytes_acked;    /* absolute offset, in [start, end] */
} range_assign_t;
```

One such record per connection index lives in the session, so the
`min(client, server)` clamp of Section 5.2.2 applies per work item rather than
to a single whole-session cursor.

## 7. Protocol-wide invariants

1. **Framing.** Exactly one `read_message()` consumes exactly one message. Short
   `recv()`/`send()` returns are looped over; `EINTR` is retried.
2. **State machine.** A server rejects any message illegal in the current
   connection state with `ERR_BAD_REQUEST` and closes.
3. **Name safety.** Any topic or file name containing `..`, a leading `/`, or
   (for file names) any `/` at all is rejected with `ERR_INVALID_NAME` before it
   reaches the filesystem.
4. **No SIGPIPE.** All sends use `MSG_NOSIGNAL` and `SIGPIPE` is ignored
   process-wide, so a vanished peer yields `EPIPE`/`ECONNRESET`, never a signal.
5. **Fault injection is server-side only** and is evaluated **once per emitted
   data chunk** (`RESP_FILE_CHUNK` or `RESP_RANGE_CHUNK`). See README,
   "Fault injection", for the definition of *p* and its consequences.
6. **Idempotent resume.** Re-sending a range the client already has is always
   safe: the client writes at absolute offsets, so a conservative retransmission
   overwrites itself rather than duplicating or appending.

## 8. Tunables and their defaults

| Setting               | Default                      | How to override                    |
|-----------------------|------------------------------|------------------------------------|
| `CHUNK_SIZE`          | 16384                        | `--chunk N` (server)               |
| `CHECKPOINT_INTERVAL` | 65536                        | `--ckpt N` (client)                |
| `SPLIT_THRESHOLD`     | 524288                       | `--split N` (Enhanced client)      |
| K parallel connections| 4                            | optional trailing positional (Enhanced client) |
| `MAX_PAYLOAD_LEN`     | 16 MiB                       | compile-time                       |
| session idle timeout  | 60 s                         | `--idle N` (server)                |
| client max retries    | 500                          | `--retries N` (client)             |
| retry backoff         | 10 ms x 2^n, capped at 500 ms| `--backoff N` (client)             |

Every tunable has a compiled-in default, so both mandatory invocations work
exactly as specified with no extra arguments.
