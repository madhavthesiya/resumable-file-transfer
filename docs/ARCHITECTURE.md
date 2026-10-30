# Architecture

This document records the decisions that had real alternatives, and why each
one went the way it did. Decisions with no alternative worth discussing are
left to the code.

---

## 1. The topic table is built before `listen()` and never mutated

**Problem.** Every request needs a topic's file list, each file's size, and its
checksum. Where does that come from?

**Alternatives.**

- *Scan the directory per request.* Simple, always current, and it puts a
  `readdir` storm plus a full CRC pass on the hot path of every connection.
- *Scan lazily and cache, with a lock.* Amortises the cost, but every request
  now takes a lock on shared mutable state — in a project whose entire point is
  measuring threading, that lock becomes the thing being measured.
- *Scan once at startup, then treat the table as immutable.* Pays the whole cost
  before the first connection exists.

**Chosen:** build once, before `listen()`.

**Consequence.** Topic lookup is a read of memory that nothing writes, so the
multi-threaded server has **zero contention** on topic metadata. The measured
threading speedup is a property of the I/O path rather than an artefact of lock
design. The cost is startup time — about 4.8 s for a 586 MB corpus of 26,179
files — and the fact that files added after startup are invisible until
restart. For a benchmark subject that trade is obviously right; for a long-lived
service it would need an inotify watch or a periodic rescan.

→ `src/common/topic_table.c:108`

---

## 2. File order is fixed by `scandir` + `alphasort`

**Problem.** The client resumes by *file index*. An index only means something
if both sides agree what file 7 is.

Directory order from `readdir` is whatever the filesystem returns — it can
differ between two reads of the same directory, and certainly differs between
filesystems. If the order changed between an interrupted transfer and its
resume, index 7 would name a different file and the client would silently
write the wrong bytes into the right filename. CRC would catch it, but only
after the whole transfer.

**Chosen:** `alphasort`, applied once when the table is built.

**Consequence.** The index is stable and portable. This is load-bearing for
resume, not a cosmetic choice.

→ `src/common/topic_table.c:44`

---

## 3. A 6-byte header with explicitly packed fields

```
 0        1        2                                  6
 +--------+--------+----------------------------------+
 |  type  | flags  |           payload_len            |  then payload_len bytes
 +--------+--------+----------------------------------+
```

**Alternatives.**

- *Write a `struct` to the socket.* One line of code, and it embeds the
  compiler's padding and the host's byte order into the wire format. A client
  built with different flags, or on a different architecture, silently
  disagrees.
- *A text protocol.* Debuggable, but it needs a parser with its own error
  cases, and framing becomes "scan for a delimiter" rather than "read 6 bytes".
- *Fixed-size binary header, fields packed byte by byte, big-endian.*

**Chosen:** the third.

**Consequence.** The receiver always knows exactly how much to read before it
knows what is arriving: read 6 bytes, then read `payload_len` bytes. Byte order
is network order, so a packet capture is readable. The macOS client and the
Linux server interoperate without a single conditional.

`payload_len` is attacker-controlled and 32 bits wide, so it is validated
against `MAX_PAYLOAD_LEN` before a single byte of payload is read — a check the
fuzzer specifically targets.

→ `src/common/protocol.c:77`

---

## 4. Every send and receive loops

`send()` on a stream socket may transfer fewer bytes than requested. So may
`recv()`. Both are wrapped once, in `io_send_all` and `io_recv_all`, which loop
until the full count has moved and retry on `EINTR`.

No other code in the project calls `send` or `recv` directly. This is the single
most common socket-programming bug, and centralising it means it can only be
got wrong in one place — which the tests then cover.

→ `src/common/protocol.c:26`

---

## 5. Position is `(file_index, byte_offset)`, and completing file 5 is `(6, 0)`

**Problem.** How do you name "how far did we get" in a stream of files?

**The trap.** The obvious encoding — "file 5, byte `size_of_file_5`" — is
ambiguous the moment a zero-length file exists. If file 6 is empty, then
`(5, size)` and `(6, 0)` describe the same point, and a resume can legitimately
skip file 6 or send it twice depending on which side's interpretation wins.

**Chosen:** a position is always the *next byte to send*. Finishing a file
advances to `(next_index, 0)`. There is exactly one representation of any point
in the stream.

**Consequence.** The comparison `pos_before(a_idx, a_off, b_idx, b_off)` is a
plain lexicographic compare with no special cases, and zero-length files need no
handling anywhere. The test corpus contains a zero-length file so that a
regression here fails immediately.

---

## 6. A resuming client is not trusted: clamp to *sent*, not *acknowledged*

**Problem.** After a break, the client reconnects and says "resume me at file 7,
byte 98304." Should the server believe it?

**No.** That number came over the network from software the server does not
control. A buggy or hostile client can claim a position beyond anything that
was ever transmitted, and the server would then begin writing at an offset the
client never filled — producing a file with a hole in it that fails CRC with no
indication of why.

**So it is clamped. To what?**

- *Clamp to the last acknowledged position.* Safe, and wasteful: it re-sends
  every byte between the last ACK and the break, data the client already has on
  disk and has already folded into its checksum.
- *Clamp to the last position actually sent.* Also safe — the client cannot
  possess bytes that were never put on the wire — and it is the tightest such
  bound.

**Chosen:** `resume_from = min(client_claimed, server_sent)`.

**Consequence.** Zero redundant bytes in every measured run, at every fault
rate, which is the sharpest correctness signal in the whole dataset: a resume
implementation that re-sends is wasteful, one that skips is corrupting, and
both failure modes are pinned to zero simultaneously.

This is a deliberate deviation from the assignment's suggested design, which
clamped to the acknowledged position. It is documented as such.

→ `src/resume/server.c:505`, high-water mark at `src/resume/session.c:161`

---

## 7. Checkpoints: write a temporary file, `fsync`, then `rename()`

**Problem.** The client must record its progress to disk often enough that a
crash is cheap, without the record itself ever being the thing that breaks.

Writing in place is not safe: a crash halfway through leaves a file that is
neither the old checkpoint nor the new one, and the next run parses garbage.

**Chosen:**

```
write  .checkpoint.tmp
fsync  .checkpoint.tmp
rename .checkpoint.tmp → .checkpoint      # atomic on POSIX
```

**Consequence.** At any instant the on-disk checkpoint is wholly the previous
one or wholly the new one. A crash costs at most one checkpoint interval of
re-transfer. **Stale is harmless; torn would not be.**

The interval is 64 KiB. Per chunk would be correct but pays an `fsync` every
16 KiB; never would make a crash cost the whole transfer. 64 KiB puts the worst
case at four chunks.

→ `src/resume/checkpoint.c:22`

---

## 8. CRC-32 is reconstructed from local disk after a resume

CRC-32 is a *running* checksum: the value after byte *n* depends on every byte
before it. A client resuming at byte 98304 cannot simply continue — it has no
running value, only the bytes it already wrote.

So it re-reads its own output file from 0 to the resume offset and folds those
bytes back through `crc32_update` to rebuild the state, then continues.

Those reads are local. They cost disk bandwidth, not network bandwidth, which
is why resume still shows **zero** redundant bytes on the wire.

The alternative — a checksum that can be resumed from a stored intermediate
state, or a per-chunk checksum instead of per-file — would avoid the re-read at
the cost of either a non-standard checksum or a weaker end-to-end guarantee.
The per-file CRC is the one the assignment specified and the one that actually
proves the file is correct.

→ `src/resume/client.c:171`

---

## 9. Parallel transfers: one thread per *client*, not per *connection*

**Problem.** In the parallel implementation a single client opens K TCP
connections. How should the server serve them?

**Alternatives.**

- *A thread per connection.* The obvious extension of the baseline design. With
  N clients × K connections the thread count multiplies, and every one of those
  threads touches the same session state, so all of it needs locking.
- *One thread per client, multiplexing its K connections with `epoll`.* The
  connections announce themselves with `REQ_ATTACH` carrying the session id;
  the accepting thread hands the socket to that session's owner thread through
  a small queue woken by an `eventfd`.

**Chosen:** the second.

**Consequence.** Thread count scales with clients, not with clients × K. More
importantly, a session is **owned outright by one thread**, so the per-range
bookkeeping needs no lock at all. File data is read with `pread`, so K
connections read different offsets of the same file without sharing a seek
pointer.

→ queue at `src/parallel/server.c:203`, `epoll_wait` at `:897`

---

## 10. Work is split into byte ranges, above a 512 KiB threshold

The client builds a plan of `(file_index, range_start, range_end)` before any
data moves. Files at or above 512 KiB are divided K ways; smaller files are
assigned whole.

The threshold exists because splitting is not free: each range costs a request,
a response, and bookkeeping on both sides. For a 30 KB file that overhead
exceeds any parallelism gained. 512 KiB is 32 protocol chunks — large enough
that the per-range cost is noise.

Output files are pre-created at full size with `ftruncate` and written with
`pwrite`, so ranges may complete in any order without the client having to
buffer or reorder anything.

→ `src/parallel/workplan.c:25`

---

## 11. Fault injection is per chunk, and seeded

The probability `p` applies to **each 16 KiB chunk**, not to each transfer. For
a topic of C chunks the expected number of faults in one pass is `C·p`, and the
expected number of passes a restart-based client needs is `(1-p)^(-C)`.

This matters more than it sounds: it makes restart's cost **exponential in
transfer length** while resume's stays linear in the number of faults, which is
what produces a computable crossover rather than a constant-factor difference.

The RNG is seeded per connection from a recorded seed, so any run can be
replayed exactly — including one that exposed a bug.

→ `src/common/fault_inject.c`

---

## 12. Retry backoff: 10 ms, doubling, capped at 500 ms, 500 attempts

Retrying instantly makes an overloaded server worse. Doubling backs off quickly
without a long first wait. The cap exists because beyond about half a second
the extra delay buys nothing against a server that is simply gone. The attempt
limit turns an unrecoverable condition into a failure with a message instead of
an infinite loop.

---

## 13. `MSG_NOSIGNAL` and macOS

Writing to a socket whose peer has closed raises `SIGPIPE`, whose default
disposition kills the process. Linux offers `MSG_NOSIGNAL` per call; macOS and
the BSDs offer `SO_NOSIGPIPE` per socket and do not define the flag at all.

```c
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif
```

Falling back to `0` is safe here specifically because every program installs
`SIG_IGN` for `SIGPIPE` before opening any socket, so the flag is belt to that
braces. This is what lets the client build and run on macOS, which CI verifies
on every push.

---

## What this design does not do

Stated plainly, because an interviewer will find them:

- **Sessions live in memory.** A server restart loses them; a client's resume is
  then refused and it starts a fresh session. Persisting them would need a
  disk-backed store with its own durability story.
- **No authentication, no encryption.** See [SECURITY.md](SECURITY.md) for the
  threat model and a TLS design sketch.
- **The parallel speedup was still climbing at K = 8.** The ceiling was never
  found, so the reported figure is a trend, not a maximum.
- **One network topology was measured.** Wireless, three hosts, one subnet.
  "Real network" and "wireless" cannot be separated from that data alone.
