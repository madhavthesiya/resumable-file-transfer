# resumable-file-transfer

A fault-tolerant, resumable file-distribution system in C11 — and a measurement
study of why benchmarking it on one machine gave the wrong answer twice.

[![CI](https://github.com/madhavthesiya/resumable-file-transfer/actions/workflows/ci.yml/badge.svg)](https://github.com/madhavthesiya/resumable-file-transfer/actions/workflows/ci.yml)
![C11](https://img.shields.io/badge/C-C11-blue)
![platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS%20client-lightgrey)
![license](https://img.shields.io/badge/license-MIT-green)

A client asks for a *topic* — a named directory — and the server streams every
file in it over TCP, each verified with CRC-32. Four implementations build on
one another, from a plain multi-threaded server to one that survives
mid-transfer connection loss and splits a single file across parallel
connections.

---

## The finding

The project's purpose was to compare restarting a broken transfer against
resuming it. The more durable result was about **measurement**: the same
benchmark, run on one machine and then on three, inverted one conclusion and
tripled another.

| Optimisation | Loopback said | Real network said | Error |
|---|---:|---:|---|
| Thread per client | **3.18×** | **1.21×** | overstated 2.6× |
| 8 parallel connections | **1.72×** | **5.33×** | understated 3.1× |

![Multi-threading advantage: loopback vs a real network](docs/results/baseline_loopback_vs_3machine_advantage.svg)

Both numbers come from the same binaries and the same workload. Only the
bottleneck changed. Loopback has no propagation delay, so it flatters anything
that hides CPU work and penalises anything that exists to hide latency —
which is exactly backwards from deployment.

**Had this been tested on one laptop, two wrong conclusions would have shipped:**
*"threading is a huge win"* and *"parallel connections aren't worth the
complexity."*

### Restart vs resume

The original question, answered. Same fault rate, same 24 MB topic, faults
injected per 16 KiB chunk at p = 0.002:

| | restart | resume |
|---|---:|---:|
| Bytes on the wire | 498.4 MB | **24.4 MB** |
| Redundant bytes re-sent | 471.8 MB | **0** |
| Completion time | 52.0 s | **3.33 s** |
| Protocol overhead | 1956 % | **0.77 %** |

Not zero *"approximately"* — **zero**, at every fault rate tested, across 50
runs with up to 96 reconnects.

But resume is not free. With **no** faults it costs **6.4× throughput**
(2384 → 375 Mbit/s) because of per-chunk acknowledgements and fsync'd
checkpoints. The crossover is computable:

```
resume wins when   e^(C·p) > 6.4   ⇒   C·p > 1.85
```

For this topic (C = 1551 chunks) that predicts p ≈ 0.0012. Measured, the
winner flips between p = 0.001 and p = 0.002 — **the prediction lands inside
the measured interval.** See [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

---

## Quick start

```bash
git clone https://github.com/madhavthesiya/resumable-file-transfer
cd resumable-file-transfer
make                      # builds all four implementations
make test                 # generates a corpus, runs every suite
```

Nothing to install beyond a C compiler and Python 3. The test corpus is
generated, not vendored.

**Run it:**

```bash
make demo-dataset
./src/baseline/server 34000 .dataset/demo multi &
./src/baseline/client 127.0.0.1 34000 images /tmp/out
kill %1                                # stop the backgrounded server
```

The client prints a `RESULT` line with bytes moved, elapsed time, throughput and
`crc_fail=0`. The host may be a DNS name as well as an address.

**Three nodes without three machines:**

```bash
docker compose up --build              # 1 server, 2 client containers
docker compose --profile faults up     # restart vs resume, side by side
```

---

## How it works

```
                    ┌──────────────────────────────────────────┐
                    │                 SERVER                   │
                    │                                          │
   built BEFORE     │   ┌──────────────────────────────────┐   │
   listen(), then   │   │   topic table  (immutable)       │   │
   never mutated:   │   │   scandir + alphasort  → index   │   │
   no lock on the   │   │   CRC-32 of every file, once     │   │
   request path     │   └───────────────┬──────────────────┘   │
                    │                   │ lock-free lookup     │
                    │   ┌───────────────┴──────────────────┐   │
                    │   │   accept()  →  thread per client │   │
                    │   └───────────────┬──────────────────┘   │
                    │                   │                      │
                    │   ┌───────────────┴──────────────────┐   │
                    │   │  session table  (mutex, 8-byte   │   │
                    │   │  id, sent/acked positions,       │   │
                    │   │  60 s idle eviction)             │   │
                    │   └───────────────┬──────────────────┘   │
                    └───────────────────┼──────────────────────┘
                                        │
             6-byte header, big-endian  │  [type|flags|payload_len]
             16 KiB payload chunks      │  + payload
                                        │
                    ┌───────────────────┴──────────────────────┐
                    │                 CLIENT                   │
                    │   write → running CRC-32 → disk          │
                    │   checkpoint every 64 KiB:               │
                    │     write .tmp → fsync → rename()        │
                    │   on break: reconnect, claim position    │
                    └──────────────────────────────────────────┘
```

### The four implementations

Each adds one idea to the one before it. They are separate programs rather than
one binary behind flags, so each reads start to finish without tracing
conditionals, and no benchmark comparison shares a code path that a flag might
skip.

How much that costs in code is itself a result: adding fault handling to the
533-line baseline server changed 343 of its lines, because the fault path has
to thread through every send and every partial write. **Resumability is not a
feature you bolt on.**

| Directory | Adds | Behaviour on a broken connection |
|---|---|---|
| [`src/baseline/`](src/baseline/) | thread per client | fails |
| [`src/restart/`](src/restart/) | probabilistic fault injection | deletes everything, starts at file 0 |
| [`src/resume/`](src/resume/) | sessions + checkpoints | continues from the last checkpoint |
| [`src/parallel/`](src/parallel/) | K connections, byte ranges | retries only the broken range |

---

## Four decisions worth defending

**1. A resuming client is not trusted.**
It reports a position; the server takes `min(claimed, actually_sent)`. Clamping
to *sent* rather than *acknowledged* is the tightest bound that is still safe —
the client cannot hold bytes that were never transmitted — and it avoids
re-sending data the client already has on disk.
→ [`src/resume/server.c:505`](src/resume/server.c#L505)

**2. Position is `(file_index, byte_offset)`, and finishing file 5 is `(6, 0)`.**
Never `(5, size)`. If file 6 were zero-length the two would be indistinguishable
and a resume could skip or duplicate a file. One canonical form deletes the
whole bug class. The test corpus contains a zero-length file for exactly this
reason.

**3. Checkpoints are written `.tmp` → `fsync` → `rename()`.**
`rename()` is atomic on POSIX, so the on-disk checkpoint is always wholly old
or wholly new. A crash mid-write costs at most one 64 KiB interval of progress.
Stale is safe; torn is not.
→ [`src/resume/checkpoint.c:22`](src/resume/checkpoint.c#L22)

**4. One thread per *client*, not per *connection*.**
In the parallel implementation K connections carry one session id and are served
by a single owner thread running `epoll_wait` over all of them, reading with
`pread` so no seek pointer is shared. Per-connection threads would multiply
thread count by K and force locking on session state that is otherwise owned
outright.
→ [`src/parallel/server.c:897`](src/parallel/server.c#L897)

Full rationale, including the alternatives rejected, in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## Correctness

| | |
|---|---|
| Benchmark runs recorded | **392** (340 loopback, 52 across three machines) |
| Files received and CRC-verified | **2,935,076** |
| CRC mismatches | **0** |
| Redundant bytes, resume implementation | **0**, at every fault rate |
| Automated checks | **366** — 120 unit, 246 across four functional suites |
| Sanitizers | ASan, UBSan, TSan in CI |
| Fuzzing | protocol decoders, standalone + libFuzzer, in CI |

The fuzzer targets the only code that consumes attacker-chosen bytes: the
payload readers, the framing layer, and the name-safety check that stops a
filename on the wire from escaping its directory.

```bash
make fuzz FUZZ_SECONDS=60
```

---

## Known limitations

Stated here rather than left to be discovered:

- **Sessions live in memory.** A server restart loses them; the client's resume
  is then refused and it starts a fresh session. Persisting them needs a
  disk-backed store with its own durability story.
- **No authentication or encryption.** CRC-32 detects accidental corruption, not
  tampering — it is trivially forgeable. [docs/SECURITY.md](docs/SECURITY.md)
  has the threat model and where TLS would attach.
- **IPv4 only.** The server binds `AF_INET`; the client resolves A records only.
- **The parallel speedup had not plateaued at K = 8**, so that figure is a trend
  rather than a maximum.
- **One network topology was measured.** Wireless, three hosts, one subnet —
  so "real network" and "wireless" cannot be separated from this data alone.

---

## Repository layout

```
src/common/      wire protocol, sockets, CRC-32, topic table, fault injection
src/baseline/    threading comparison
src/restart/     fault injection, restart-from-zero client
src/resume/      session management, checkpoints, resume
src/parallel/    work plan, K connections, epoll owner thread
tests/           unit tests + one functional suite per implementation
fuzz/            protocol decoder fuzzer
bench/           benchmark harness, summariser, plotting
tools/           deterministic dataset generator
docs/            protocol spec, architecture, benchmarks, findings, security
```

---

## Documentation

| | |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | design decisions and the alternatives rejected |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | wire format, message catalogue, state machine |
| [docs/BENCHMARKS.md](docs/BENCHMARKS.md) | method, all results, the crossover derivation |
| [docs/FINDINGS.md](docs/FINDINGS.md) | four ways the measurements lied, and how each was caught |
| [docs/SECURITY.md](docs/SECURITY.md) | threat model, what is and is not defended, TLS design |

---

## Origin and credits

Built as the term project for IT305 (Computer Networks) by a six-person team
formed from lab groups 88 and 92.

**Madhav Thesiya** led the project and owned the wire protocol, the session and
resume design, the benchmark harness, and the three-machine study.
Contributions from Meet Virugama (topic table, threading), Krish (fault
injection), Pritkumar (session state), Om (checkpoint persistence) and Shlok
(parallel connections) are recorded in
[docs/contributions.csv](docs/contributions.csv).

The original graded submission and its report are preserved in
[docs/academic-report.pdf](docs/academic-report.pdf). This repository
restructures that work for public use: the implementations were renamed from
their assignment labels, the build and test setup was made self-contained, and
CI, containerisation, and fuzzing were added.

MIT licensed — see [LICENSE](LICENSE).
