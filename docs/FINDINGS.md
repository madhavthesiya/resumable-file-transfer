# Four ways the measurements lied

Every early number this project produced was wrong, and never for an
interesting reason. None of these were subtle bugs in the networking code. All
of them would have produced a clean, confident, wrong report.

They are written down because catching them was the most valuable work in the
project, and because the fixes are cheap once you know to look.

---

## 1. One run per configuration

**The symptom.** Two runs of the *same* configuration — same binary, same
topic, same machine, nothing changed between them — produced **1690 Mbit/s** and
**583 Mbit/s**. A 2.9× spread with zero independent variables.

**What was actually being measured.** The scheduler. With a single sample per
configuration, run-to-run variance was indistinguishable from the effect under
study. Any conclusion drawn from that table would have been noise with a
decimal point.

**The fix.** Ten repeats per configuration on loopback, five on the slower
three-machine setup, and report the standard deviation alongside every mean.

**Why it is worth stating.** Look at restart under faults at p = 0.002:

```
52.01 s ± 43.92
```

The deviation is 84% of the mean. That is not sloppy measurement — it is the
*result*. Restart's completion time depends on the worst luck in the entire
run, so it is not merely slow, it is **unpredictable**, which for a real system
is often worse. A single-sample table would have reported "52 s" and hidden the
most interesting property of the design.

---

## 2. Run order, through the page cache

**The symptom.** Preparing the live demonstration, the single-threaded round
with two clients took **12 seconds**. The multi-threaded round that followed
took **1 second**. A 12× speedup — against the 2–3× that ten repeats had
established.

**The cause.** The single-threaded round ran first, with a cold page cache. The
server read all 4,863 files from disk. The multi-threaded round then read the
same files from RAM.

Re-running the single-threaded round once the cache was warm:

| | makespan |
|---|---:|
| single-threaded, cold cache | 12 s |
| single-threaded, warm cache | **5 s** |
| multi-threaded, warm cache | 1 s |

**More than half the apparent advantage was disk caching, attributable purely
to which test ran first.**

**The fix.** A warm-up transfer before any timed run, and — for the
three-machine study — interleaving the two modes *within* each repeat rather
than running all of one and then all of the other, so that drifting wireless
conditions affect both equally. Clients are dealt alternately to the two client
machines for the same reason.

The first distributed pass was run before this was understood. It is kept in
the repository as `docs/results/distributed_pass1/`, labelled, with a README
explaining the bias — deleting it would hide a real result about method.

---

## 3. A clock that ran backwards

**The symptom.** One run reported an elapsed time of **−0.42 seconds**.

**The cause.** The harness computed makespan from wall-clock timestamps taken
before and after the run. Under WSL2, `CLOCK_REALTIME` is periodically
corrected against the host and can step backwards. Subtracting two such
timestamps can yield a negative interval.

**The fix.** Two changes. The client's own elapsed time uses a monotonic clock,
which cannot step backwards. The harness keeps the wall-clock span but falls
back to the maximum client-reported elapsed time when the two disagree, and
**records both figures in the CSV** so the anomaly stays visible rather than
being quietly smoothed over.

```bash
wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.6f", b-a}')
maxel=$(max_field_f elapsed_s "${ress[@]}")
makespan=$(awk -v w="$wall" -v e="$maxel" \
           'BEGIN{printf "%.6f", (w > 0 && w >= e) ? w : e}')
```

A negative duration is obviously wrong and therefore easy to catch. The
uncomfortable question is how many *plausible* durations the same clock had
already distorted by a few percent.

---

## 4. Loopback itself

The first three are mistakes. This one is structural, and it is the project's
central result.

Every measurement was first taken on one machine, client and server talking
over loopback. Repeating the identical benchmark across three physical hosts:

| Optimisation | Loopback | Three machines | |
|---|---:|---:|---|
| Thread per client | 3.18× | **1.21×** | overstated 2.6× |
| 8 parallel connections | 1.72× | **5.33×** | understated 3.1× |

Same binaries. Same workload. Opposite errors.

**Why.** An optimisation's measured value depends entirely on which resource is
the bottleneck, and loopback's bottleneck is not the one any deployment has.

- Loopback has effectively no propagation delay, and on 18 cores there is CPU
  to spare. **Threading** hides CPU work, so it looks excellent — until the
  bottleneck moves to a link that one thread already saturates.
- **Parallel connections** exist to fill the gaps a single TCP stream leaves
  while waiting for acknowledgements. On loopback there are no gaps, so K
  connections contend for the same CPU and pay coordination overhead for
  nothing. At K = 8 the loopback result was *worse* than K = 4.

Judging either feature on loopback gives the wrong answer. Judging both gives
two wrong answers that point in opposite directions, so not even the *ranking*
survives.

A third environment — wired gigabit in a lab — is expected to differ again,
and for the same reason.

---

## Bonus: a test that could not fail

While verifying the submission against a simulated auto-grader, one check was
written like this:

```bash
if grep -rnE '"/home/|"/mnt/' "$S"/src/*.c | head -3; then
    fail "a hard-coded path appears in the sources"
else
    pass "no hard-coded paths"
fi
```

It had two defects. `head` exits 0 whether or not it printed anything, so the
`if` always took the failure branch regardless of what `grep` found. And
`grep -rn` prints the file *path* alongside each match, so running the check
from inside `/home/...` made every file match the pattern `"/home/`.

The check reported a failure that did not exist, for a reason unrelated to what
it was testing. Fixed:

```bash
hits=$(grep -nE '"(/home|/mnt|/Users)/' $ALL 2>/dev/null | head -5)
if [ -n "$hits" ]; then fail "..."; else pass "..."; fi
```

**A green test proves nothing until the test has been shown capable of going
red.** This one never could.

---

## What to take from this

In performance work the default state of a measurement is *wrong*, and the
things that catch it are unglamorous:

1. **Repeat every configuration** and report the spread. A result without a
   deviation is an anecdote.
2. **Vary the order** of the conditions, or the order becomes a variable.
3. **Use a monotonic clock**, and keep both figures when two sources of truth
   disagree.
4. **Measure in the environment you are reasoning about.** Loopback is a
   different machine with a different bottleneck, not a faster version of the
   same one.
5. **Make a passing test fail on purpose once**, before trusting it.
