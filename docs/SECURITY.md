# Security

This is a benchmark subject and a teaching system, not a hardened service. The
honest position is written down here rather than left implied, because "it was
only a course project" is not a threat model.

---

## Threat model

The server accepts connections from anyone who can reach its port and serves
files from a directory tree the operator nominates. It assumes:

- the **operator** is trusted and chooses the dataset root deliberately
- the **network** is not trusted — any byte on the wire may be chosen by an
  attacker
- the **client** is not trusted — it is software the server does not control,
  and nothing it says may be believed without checking

The last point is the one that shapes the code.

---

## What is defended

### Path traversal

A filename arrives over the wire and is joined onto the dataset directory. If
`../../etc/passwd` were accepted, the server would happily serve it.

`name_safe_topic()` and `name_safe_file()` reject the empty string, anything
containing `/`, the names `.` and `..`, and anything longer than the
destination buffer. Both are called before a name is used in a path, and both
are a fuzz target.

→ `src/common/protocol.c:356`

### Attacker-controlled lengths

The 6-byte header carries a 32-bit `payload_len` chosen by the peer. "Declare
4 GB" is the obvious first attack: a receiver that trusts it either allocates
absurdly or overruns a fixed buffer.

`msg_recv_hdr()` validates `payload_len` against `MAX_PAYLOAD_LEN` **before a
single payload byte is read**, and returns an error rather than attempting the
read. The fuzzer asserts this invariant directly.

→ `src/common/protocol.c:147`

### A lying resume position

A client reconnecting after a fault claims a byte offset to resume from. A
hostile client could claim a position past anything that was sent, and a naive
server would begin writing there, leaving a hole in the file.

The server clamps: `resume_from = min(client_claimed, server_sent)`. The client
cannot possess bytes that were never transmitted, so this is both safe and the
tightest bound available.

→ `src/resume/server.c:505`

### Payload underflow

The payload readers (`pv_u8`, `pv_u32`, `pv_bytes`, …) set a sticky `bad` flag
on any read past the end and never return out-of-bounds data. `pv_done()`
additionally requires that the payload was consumed *exactly* — a payload
longer than the message type defines is treated as malformed rather than
silently ignored, so a trailing-data smuggling attempt fails closed.

### Truncated and torn state

Checkpoints are written to a temporary file and `rename()`d into place, so a
crash — or a process killed mid-write — can never leave a partially written
checkpoint that the next run would parse as valid.

### SIGPIPE

Every program installs `SIG_IGN` for `SIGPIPE` before opening a socket, so a
peer disconnecting mid-write cannot kill the process. A fault-injection system
that could be crashed by its own faults would not be much of a test.

---

## What is **not** defended

Stated plainly:

| Gap | Consequence |
|---|---|
| **No authentication** | anyone who can reach the port can request any topic |
| **No encryption** | file contents and names are plaintext on the wire |
| **No integrity against tampering** | CRC-32 detects accidental corruption, not deliberate modification — it is trivially forgeable |
| **No rate limiting or connection cap** | a client may open connections until the server runs out of descriptors |
| **No per-client quota** | one client can saturate the server |
| **Sessions are guessable only by chance** | the 8-byte id is from `random()`, not a CSPRNG; it is a collision guard, not a capability token |
| **No resource bound on sessions** | they expire after 60 s idle, but a client can create them faster than they expire |

**The session id deserves emphasis.** It is generated with the C library PRNG
and is adequate for its actual job — distinguishing concurrent transfers. It is
*not* a secret. If sessions ever became a capability ("present this id, resume
this transfer"), it would need to come from `getrandom()` and be compared in
constant time.

---

## Fuzzing

The decoders are the only code that consumes bytes chosen by someone else, so
they are the only place a malformed input becomes a memory-safety bug. Three
targets, run in CI on every push:

| Target | Asserts |
|---|---|
| payload readers | the cursor never passes the end; `pv_bytes` returns NULL exactly on underflow; the `bad` flag is sticky |
| framing | `payload_len` above the limit is always rejected; the status is always one of the defined values |
| name safety | any name containing `/`, or equal to `.`/`..`/`""`, is refused |

```bash
make fuzz FUZZ_SECONDS=60          # standalone, ASan + UBSan, no clang needed
make -C fuzz run-libfuzzer         # coverage-guided, needs clang
```

Both builds share one `fuzz_one()` entry point, so a crash found by either is
reproducible by the other. The standalone driver prints its seed and replays
from `FUZZ_SEED=`.

ASan, UBSan and TSan also run over the unit tests and an end-to-end transfer in
CI.

---

## If this were to be deployed

Roughly in order of value:

1. **TLS.** Terminate with OpenSSL or rustls rather than inventing anything. The
   natural seam is `io_send_all`/`io_recv_all` in `src/common/protocol.c` —
   every byte already passes through those two functions, so a TLS transport
   would replace them and nothing above layer 1 would change. That is a direct
   consequence of having centralised the partial-read loops.

2. **Authentication before topic access.** A pre-shared key or client
   certificate checked at `REQ_TOPIC` time. Note that resume makes this subtler
   than it looks: `REQ_SESSION_RESUME` arrives on a *new* connection, so the
   session id would have to be bound to the authenticated identity, not just to
   the transfer.

3. **A CSPRNG for session ids**, from `getrandom(2)`, once they carry any
   authority at all.

4. **Replace CRC-32 with a cryptographic hash** if the threat is tampering
   rather than corruption. CRC-32 is the right tool for detecting a flipped bit
   and the wrong tool for detecting an adversary; it is cheap and resumable,
   which is why it is used here.

5. **Connection and session caps**, plus a per-IP rate limit.

6. **Drop privileges and chroot** (or a mount namespace) around the dataset
   root, so that a path-handling bug has nothing to reach.

---

## Reporting

This is a portfolio project with no deployment and no users. If you find
something interesting in it anyway, open an issue — a demonstrated
memory-safety bug in the decoders would be a genuinely good find, and the
fuzzer corpus that produced it is the most useful thing to attach.
