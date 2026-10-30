#!/bin/bash
# Functional tests for Part2 Case 1 (fault injection, no session management).
#
#   ./test_restart.sh [topics_root] [work_dir]
#
# Checks that: p=0 behaves exactly like Part I; p>0 still delivers every byte
# correctly after automatic retries; redundant bytes appear (that is the point
# of Case 1); permanent errors are not retried; and a server crash mid-transfer
# is survived.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"

ROOT=${1:-$HERE/../.dataset/synthetic}
WORK=${2:-$HERE/../.work/restart}
BIN="$HERE/../src/restart"
TOPIC=topic_alpha

rm -rf "$WORK"; mkdir -p "$WORK"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

start_server() {                       # start_server <p> [extra args...]
    local p=$1; shift
    PORT=$(free_port 30100)
    "$BIN/server" "$PORT" "$ROOT" "$p" "$@" > "$WORK/srv_p$p.log" 2>&1 &
    SRV_PID=$!
    wait_port "$PORT" || { bad "server (p=$p) failed to listen"; tail -5 "$WORK/srv_p$p.log"; return 1; }
    note "server listening on $PORT with p=$p"
}
stop_server() { kill -INT "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""; }

hdr "Case 1 - build and CLI contract"
( cd "$BIN" && make clean >/dev/null && make > "$WORK/build.log" 2>&1 )
elf=0
for f in "$BIN"/*; do
    [ -f "$f" ] || continue
    [ "$(head -c4 "$f" | od -An -tx1 | tr -d "[:space:]")" = "7f454c46" ] && elf=$((elf+1))
done
check_eq "make produced exactly 2 ELF executables" "2" "$elf"
# The failure probability is mandatory for Part 2, so two args must be refused.
"$BIN/server" 31999 "$ROOT" >/dev/null 2>&1
check_eq "server without failure_probability exits 2" "2" "$?"
"$BIN/server" 31999 "$ROOT" 1.5 >/dev/null 2>&1
check_eq "server rejects p=1.5" "2" "$?"
"$BIN/server" 31999 "$ROOT" abc >/dev/null 2>&1
check_eq "server rejects a non-numeric p" "2" "$?"

hdr "Case 1 - p=0 must behave exactly like Part I"
start_server 0 --quiet || exit 1
"$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/p0" --quiet > "$WORK/p0.out" 2>&1
check_eq "client exit status" "0" "$?"
check_eq "status"             "ok" "$(result_field "$WORK/p0.out" status)"
check_eq "reconnects at p=0"  "0"  "$(result_field "$WORK/p0.out" reconnects)"
check_eq "redundant bytes at p=0" "0" "$(result_field "$WORK/p0.out" redundant_bytes)"
check_eq "crc failures"       "0"  "$(result_field "$WORK/p0.out" crc_fail)"
compare_trees "$ROOT/$TOPIC" "$WORK/p0/$TOPIC" "p=0 output"
stop_server

hdr "Case 1 - an unknown topic is fatal, not retried"
start_server 0 --quiet || exit 1
t0=$(date +%s%N)
"$BIN/client" 127.0.0.1 "$PORT" nope "$WORK/bad" --quiet > "$WORK/bad.out" 2>"$WORK/bad.err"
check_eq "unknown topic exits 1" "1" "$?"
t1=$(date +%s%N)
ms=$(( (t1 - t0) / 1000000 ))
if [ "$ms" -lt 2000 ]; then ok "failed fast (${ms}ms, so it did not enter the retry loop)"
else bad "took ${ms}ms: it looks like a permanent error was retried"; fi
check_eq "attempts for a permanent error" "1" "$(result_field "$WORK/bad.out" attempts)"
stop_server

hdr "Case 1 - faults at several probabilities (every byte must still arrive)"
# topic_alpha is 5400675 bytes = 330 chunks of 16 KiB, so the chance of a
# fault-free pass is (1-p)^330.  The p values below are chosen so Case 1 can
# still finish: at p=0.01 that is 3.6%, i.e. about 28 attempts on average.
# Much larger p makes Case 1 diverge, which is exactly the point the report
# makes and the reason the benchmark uses far smaller p on the real dataset.
for p in 0.001 0.003 0.01; do
    start_server "$p" --quiet --seed 777 || exit 1
    out="$WORK/p$p"
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$out" --quiet > "$WORK/p$p.out" 2>"$WORK/p$p.err"
    rc=$?
    check_eq "p=$p: client exit status" "0" "$rc"
    check_eq "p=$p: status"             "ok" "$(result_field "$WORK/p$p.out" status)"
    check_eq "p=$p: crc failures"       "0"  "$(result_field "$WORK/p$p.out" crc_fail)"
    compare_trees "$ROOT/$TOPIC" "$out/$TOPIC" "p=$p output"

    rec=$(result_field "$WORK/p$p.out" reconnects)
    red=$(result_field "$WORK/p$p.out" redundant_bytes)
    att=$(result_field "$WORK/p$p.out" attempts)
    note "p=$p: $att attempt(s), $rec reconnect(s), $red redundant bytes"
    if [ "${rec:-0}" -gt 0 ]; then ok "p=$p: faults actually occurred and were recovered"
    else note "p=$p: no fault fired this run (possible at low p)"; fi
    if [ "${rec:-0}" -gt 0 ] && [ "${red:-0}" -eq 0 ]; then
        bad "p=$p: reconnected but reported 0 redundant bytes"
    fi
    # The server's own log must record the injected faults.
    sf=$(grep -c "FAULT injected" "$WORK/srv_p$p.log" || true)
    note "p=$p: server logged $sf injected fault(s)"
    stop_server
done

hdr "Case 1 - concurrent clients with faults"
start_server 0.002 --quiet --seed 42 || exit 1
pids=(); n=4
for ((i = 0; i < n; i++)); do
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/conc$i" --quiet \
        > "$WORK/conc$i.out" 2> "$WORK/conc$i.err" &
    pids+=($!)
done
allok=1
for p in "${pids[@]}"; do wait "$p" || allok=0; done
if [ $allok -eq 1 ]; then ok "all $n concurrent clients completed"
else bad "a concurrent client failed"; tail -5 "$WORK"/conc*.err; fi
badtrees=0
for ((i = 0; i < n; i++)); do
    diff -r "$ROOT/$TOPIC" "$WORK/conc$i/$TOPIC" >/dev/null 2>&1 || badtrees=$((badtrees+1))
done
check_eq "every concurrent output tree is byte-identical" "0" "$badtrees"
tot=0
for ((i = 0; i < n; i++)); do
    r=$(result_field "$WORK/conc$i.out" reconnects); tot=$((tot + ${r:-0}))
done
note "$tot reconnect(s) across $n concurrent clients"
if kill -0 "$SRV_PID" 2>/dev/null; then ok "server survived all injected faults"
else bad "server died during fault injection"; fi
stop_server

hdr "Case 1 - surviving a server crash and restart (no manual step)"
# Three things make this deterministic rather than racy:
#   --chunk 512    32x more messages per byte, so the transfer takes longer;
#   --sndbuf/--rcvbuf 4096   small socket buffers, so the server cannot run
#                  ahead of the client and finish while the kill is in flight
#                  (on loopback the buffers otherwise autotune into megabytes,
#                  and a 5.9 MB topic was being fully buffered before the kill
#                  landed - the first version of this test passed without ever
#                  exercising the recovery path);
#   the kill is triggered by observing real progress, not a fixed sleep.
start_server 0 --quiet --chunk 512 --sndbuf 4096 || exit 1
CRASH_PORT=$PORT
NFILES=$(ls -1 "$ROOT/topic_beta" | wc -l)
"$BIN/client" 127.0.0.1 "$CRASH_PORT" topic_beta "$WORK/crash" --quiet --rcvbuf 4096 > "$WORK/crash.out" 2> "$WORK/crash.err" &
CLI=$!
# Wait until the client is demonstrably mid-transfer.
mid=0
for i in $(seq 1 400); do
    got=$(ls -1 "$WORK/crash/topic_beta" 2>/dev/null | wc -l)
    if [ "$got" -ge 3 ] && [ "$got" -lt "$NFILES" ]; then mid=1; break; fi
    [ "$got" -ge "$NFILES" ] && break
    sleep 0.01
done
check_eq "client was mid-transfer when the server was killed" "1" "$mid"
kill -9 "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null
note "server killed with SIGKILL after $(ls -1 "$WORK/crash/topic_beta" 2>/dev/null | wc -l)/$NFILES files"
if kill -0 "$CLI" 2>/dev/null; then ok "client still running after the server died"
else bad "client exited before the server was restarted"; fi
sleep 0.3
# Bring it back on the same port; the client must find it by itself.
"$BIN/server" "$CRASH_PORT" "$ROOT" 0 --quiet > "$WORK/srv_restart.log" 2>&1 &
SRV_PID=$!
wait_port "$CRASH_PORT" || bad "restarted server failed to listen"
note "server restarted on the same port"
if wait "$CLI"; then ok "client recovered from the crash without any manual step"
else bad "client did not recover from the server crash"; tail -5 "$WORK/crash.err"; fi
compare_trees "$ROOT/topic_beta" "$WORK/crash/topic_beta" "post-crash output"
rec=$(result_field "$WORK/crash.out" reconnects)
if [ "${rec:-0}" -ge 1 ]; then ok "recovery really happened ($rec reconnect(s))"
else bad "reported 0 reconnects, so the crash path was not exercised"; fi
note "attempts: $(result_field "$WORK/crash.out" attempts), reconnects: $rec"
stop_server

hdr "Case 1 - the retry cap is honoured"
# With no server at all, the client must give up after --retries attempts
# rather than looping forever.
DEADPORT=$(free_port 30500)
t0=$(date +%s%N)
"$BIN/client" 127.0.0.1 "$DEADPORT" "$TOPIC" "$WORK/dead" --quiet --retries 3 --backoff 1 \
    > "$WORK/dead.out" 2>"$WORK/dead.err"
check_eq "client with no server exits non-zero" "1" "$?"
t1=$(date +%s%N)
check_eq "gave up after exactly 4 attempts (1 + 3 retries)" "4" \
         "$(result_field "$WORK/dead.out" attempts)"
note "elapsed $(( (t1 - t0) / 1000000 ))ms for the capped retry loop"

hdr "Case 1 - reproducible faults with --seed"
# The same seed must inject the same number of faults, which is what makes two
# configurations comparable under an identical fault pattern.
runs=""
for r in 1 2; do
    start_server 0.004 --quiet --seed 20261022 || exit 1
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/seed$r" --quiet > "$WORK/seed$r.out" 2>&1
    runs="$runs $(result_field "$WORK/seed$r.out" reconnects)"
    stop_server
done
set -- $runs
check_eq "the same --seed gives the same fault count" "$1" "$2"
note "reconnect counts with a pinned seed:$runs"

summary "PART 2 CASE 1"
