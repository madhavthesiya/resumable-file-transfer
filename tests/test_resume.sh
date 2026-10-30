#!/bin/bash
# Functional tests for Part2 Case 2 (fault injection WITH session management).
#
#   ./test_resume.sh [topics_root] [work_dir]
#
# The headline check is quantitative: after N faults the redundant bytes must
# be bounded by roughly N checkpoint intervals, not by N whole attempts as in
# Case 1.  That bound is the entire justification for session management, so it
# is asserted rather than eyeballed.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"

ROOT=${1:-$HERE/../.dataset/synthetic}
WORK=${2:-$HERE/../.work/resume}
BIN="$HERE/../src/resume"
TOPIC=topic_alpha

rm -rf "$WORK"; mkdir -p "$WORK"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

start_server() {                       # start_server <p> [extra args...]
    local p=$1; shift
    PORT=$(free_port 30600)
    "$BIN/server" "$PORT" "$ROOT" "$p" "$@" > "$WORK/srv_$p.log" 2>&1 &
    SRV_PID=$!
    wait_port "$PORT" || { bad "server (p=$p) failed to listen"; tail -5 "$WORK/srv_$p.log"; return 1; }
    note "server listening on $PORT with p=$p"
}
stop_server() { kill -INT "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""; }

hdr "Case 2 - build and CLI contract"
( cd "$BIN" && make clean >/dev/null && make > "$WORK/build.log" 2>&1 )
elf=0
for f in "$BIN"/*; do
    [ -f "$f" ] || continue
    [ "$(head -c4 "$f" | od -An -tx1 | tr -d "[:space:]")" = "7f454c46" ] && elf=$((elf+1))
done
check_eq "make produced exactly 2 ELF executables" "2" "$elf"
"$BIN/server" 31998 "$ROOT" >/dev/null 2>&1
check_eq "server without failure_probability exits 2" "2" "$?"

hdr "Case 2 - p=0 behaves like Part I and leaves no checkpoint behind"
start_server 0 --quiet || exit 1
"$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/p0" --quiet > "$WORK/p0.out" 2>&1
check_eq "client exit status" "0" "$?"
check_eq "status"                  "ok" "$(result_field "$WORK/p0.out" status)"
check_eq "reconnects at p=0"       "0"  "$(result_field "$WORK/p0.out" reconnects)"
check_eq "redundant bytes at p=0"  "0"  "$(result_field "$WORK/p0.out" redundant_bytes)"
check_eq "crc failures"            "0"  "$(result_field "$WORK/p0.out" crc_fail)"
compare_trees "$ROOT/$TOPIC" "$WORK/p0/$TOPIC" "p=0 output"
if [ -f "$WORK/p0/$TOPIC/.checkpoint" ]; then
    bad "a .checkpoint file was left behind after a successful transfer"
else
    ok "no .checkpoint left behind after success"
fi
# compare_trees uses diff -r, which would have flagged a stray .checkpoint, so
# its absence is also confirmed independently above.
note "ACKs sent: $(result_field "$WORK/p0.out" acks), checkpoints written: $(result_field "$WORK/p0.out" ckpts)"
stop_server

hdr "Case 2 - faults are resumed, and redundancy stays bounded"
for p in 0.001 0.005 0.02; do
    start_server "$p" --quiet --seed 555 || exit 1
    out="$WORK/p$p"
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$out" --quiet > "$WORK/p$p.out" 2>"$WORK/p$p.err"
    rc=$?
    check_eq "p=$p: client exit status" "0" "$rc"
    check_eq "p=$p: status"             "ok" "$(result_field "$WORK/p$p.out" status)"
    check_eq "p=$p: crc failures"       "0"  "$(result_field "$WORK/p$p.out" crc_fail)"
    compare_trees "$ROOT/$TOPIC" "$out/$TOPIC" "p=$p output"

    rec=$(result_field "$WORK/p$p.out" reconnects)
    red=$(result_field "$WORK/p$p.out" redundant_bytes)
    res=$(result_field "$WORK/p$p.out" resumes)
    note "p=$p: $rec fault(s)/reconnect(s), $res resume handshake(s), $red redundant bytes"

    # Each fault can cost at most one checkpoint interval (64 KiB) of rework,
    # plus one chunk (16 KiB) of slack.  Case 1 at the same fault count would
    # re-send whole multi-megabyte attempts instead.
    if [ "${rec:-0}" -gt 0 ]; then
        ok "p=$p: faults occurred and were resumed"
        limit=$(( rec * (65536 + 16384) ))
        if [ "${red:-0}" -le "$limit" ]; then
            ok "p=$p: redundancy $red <= bound $limit ($rec x 80 KiB)"
        else
            bad "p=$p: redundancy $red exceeds the $limit byte bound"
        fi
        if [ "${res:-0}" -lt 1 ]; then
            bad "p=$p: reconnected but performed no resume handshake"
        fi
    else
        note "p=$p: no fault fired this run"
    fi
    stop_server
done

hdr "Case 2 - killing the CLIENT mid-transfer and restarting it resumes"
# This is the test the checkpoint exists for: the client process dies, so its
# in-memory state is gone and only the on-disk record can save the work.
start_server 0 --quiet --chunk 512 --sndbuf 4096 || exit 1
KPORT=$PORT
NF=$(ls -1 "$ROOT/topic_beta" | wc -l)
"$BIN/client" 127.0.0.1 "$KPORT" topic_beta "$WORK/kill" --quiet --rcvbuf 4096 \
    > "$WORK/kill1.out" 2> "$WORK/kill1.err" &
CLI=$!
mid=0
for i in $(seq 1 500); do
    got=$(ls -1 "$WORK/kill/topic_beta" 2>/dev/null | grep -cv '^\.checkpoint' || true)
    if [ "$got" -ge 5 ] && [ "$got" -lt "$NF" ]; then mid=1; break; fi
    [ "$got" -ge "$NF" ] && break
    sleep 0.01
done
check_eq "client reached mid-transfer before being killed" "1" "$mid"
kill -9 "$CLI" 2>/dev/null; wait "$CLI" 2>/dev/null
part=$(ls -1 "$WORK/kill/topic_beta" | grep -cv '^\.checkpoint' || true)
note "client killed with SIGKILL after $part/$NF files"

if [ -f "$WORK/kill/topic_beta/.checkpoint" ]; then
    ok "a .checkpoint survived the SIGKILL"
    note "checkpoint contents:"
    sed 's/^/        /' "$WORK/kill/topic_beta/.checkpoint"
    ck_idx=$(sed -n 's/^file_idx=//p' "$WORK/kill/topic_beta/.checkpoint")
    if [ "${ck_idx:-0}" -ge 1 ]; then ok "checkpoint records real progress (file_idx=$ck_idx)"
    else bad "checkpoint records no progress"; fi
    if grep -q '^crc32=' "$WORK/kill/topic_beta/.checkpoint"; then
        ok "checkpoint carries its CRC line"
    else bad "checkpoint has no CRC line"; fi
else
    bad "no .checkpoint was written before the client was killed"
fi

# Restart the client with the identical command line; it must resume by itself.
"$BIN/client" 127.0.0.1 "$KPORT" topic_beta "$WORK/kill" --quiet --rcvbuf 4096 \
    > "$WORK/kill2.out" 2> "$WORK/kill2.err"
check_eq "restarted client exit status" "0" "$?"
check_eq "restarted client status"      "ok" "$(result_field "$WORK/kill2.out" status)"
res2=$(result_field "$WORK/kill2.out" resumes)
if [ "${res2:-0}" -ge 1 ]; then ok "the restart resumed an existing session ($res2 resume)"
else bad "the restart did not resume; it started over"; fi
# The resumed run must have fetched strictly less than the whole topic.
total=$(cd "$ROOT/topic_beta" && du -cb . | tail -1 | cut -f1)
got2=$(result_field "$WORK/kill2.out" payload_bytes)
if [ "${got2:-0}" -lt "${total:-0}" ]; then
    ok "the restart transferred only $got2 of $total bytes (it really resumed)"
else
    bad "the restart transferred $got2 bytes of a $total byte topic: no saving"
fi
compare_trees "$ROOT/topic_beta" "$WORK/kill/topic_beta" "resumed-after-kill output"
stop_server

hdr "Case 2 - a corrupt checkpoint is detected and ignored"
start_server 0 --quiet || exit 1
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/corrupt" --quiet >/dev/null 2>&1
# Forge a checkpoint whose CRC line does not match its body.
mkdir -p "$WORK/corrupt/topic_gamma"
printf 'magic=IT305-CKPT1\nsession=0000000000001234\ntopic=topic_gamma\nfile_count=5\nfile_idx=2\nbyte_offset=999\ncrc32=deadbeef\n' \
    > "$WORK/corrupt/topic_gamma/.checkpoint"
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/corrupt" > "$WORK/corrupt.out" 2>"$WORK/corrupt.err"
check_eq "a corrupt checkpoint does not stop the client" "0" "$?"
if grep -q "failed its CRC check" "$WORK/corrupt.err"; then
    ok "the corrupt checkpoint was detected by its CRC and ignored"
else
    bad "the corrupt checkpoint was not reported"; tail -3 "$WORK/corrupt.err"
fi
compare_trees "$ROOT/topic_gamma" "$WORK/corrupt/topic_gamma" "output after a corrupt checkpoint"

hdr "Case 2 - a stale session is refused and the client falls back to a fresh one"
# Point a valid-looking checkpoint at a session id the server has never heard
# of.  The server must refuse, and the client must start over by itself.
rm -rf "$WORK/stale"; mkdir -p "$WORK/stale/topic_gamma"
body='magic=IT305-CKPT1\nsession=00000000deadbee1\ntopic=topic_gamma\nfile_count=5\nfile_idx=1\nbyte_offset=0\n'
printf "$body" > "$WORK/stale/topic_gamma/.body"
crc=$(printf "$body" | python3 -c 'import sys,zlib;print("%08x"%zlib.crc32(sys.stdin.buffer.read()))')
{ printf "$body"; printf 'crc32=%s\n' "$crc"; } > "$WORK/stale/topic_gamma/.checkpoint"
rm -f "$WORK/stale/topic_gamma/.body"
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/stale" > "$WORK/stale.out" 2>"$WORK/stale.err"
check_eq "client recovers from an unknown session" "0" "$?"
if grep -q "refused the resume" "$WORK/stale.err"; then
    ok "the server refused the stale resume and the client restarted"
else
    bad "the stale resume was not refused"; tail -5 "$WORK/stale.err"
fi
check_eq "the fallback transfer succeeded" "ok" "$(result_field "$WORK/stale.out" status)"
check_eq "one refusal was recorded" "1" "$(result_field "$WORK/stale.out" rejects)"
compare_trees "$ROOT/topic_gamma" "$WORK/stale/topic_gamma" "output after a refused resume"
stop_server

hdr "Case 2 - idle sessions are evicted"
start_server 0 --quiet --idle 1 || exit 1
IPORT=$PORT
"$BIN/client" 127.0.0.1 "$IPORT" topic_beta "$WORK/idle" --quiet --chunk 512 >/dev/null 2>&1 &
CLI=$!
sleep 0.05
kill -9 "$CLI" 2>/dev/null; wait "$CLI" 2>/dev/null
sleep 3.5                               # longer than --idle 1 plus the 1 s reap tick
if grep -qE "evicted .* idle session|evicted after" "$WORK/srv_0.log"; then
    ok "the reaper evicted the abandoned session"
else
    note "no eviction logged (the transfer may have completed before the kill)"
fi
stop_server

hdr "Case 2 - concurrent clients with faults"
start_server 0.003 --quiet --seed 99 || exit 1
pids=(); n=4
for ((i = 0; i < n; i++)); do
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/conc$i" --quiet \
        > "$WORK/conc$i.out" 2> "$WORK/conc$i.err" &
    pids+=($!)
done
allok=1
for p in "${pids[@]}"; do wait "$p" || allok=0; done
if [ $allok -eq 1 ]; then ok "all $n concurrent clients completed"
else bad "a concurrent client failed"; tail -8 "$WORK"/conc*.err; fi
badtrees=0
for ((i = 0; i < n; i++)); do
    diff -r "$ROOT/$TOPIC" "$WORK/conc$i/$TOPIC" >/dev/null 2>&1 || badtrees=$((badtrees+1))
done
check_eq "every concurrent output tree is byte-identical" "0" "$badtrees"
tr_=0; tred=0
for ((i = 0; i < n; i++)); do
    r=$(result_field "$WORK/conc$i.out" reconnects); tr_=$((tr_ + ${r:-0}))
    d=$(result_field "$WORK/conc$i.out" redundant_bytes); tred=$((tred + ${d:-0}))
done
note "$tr_ fault(s) across $n clients, $tred redundant bytes in total"
# Each client must have had its own session; a shared or crossed session would
# show up as a CRC failure or a diff, both already checked above.
sess=$(grep -c "new session" "$WORK/srv_0.003.log" || true)
note "server opened $sess session(s)"
if kill -0 "$SRV_PID" 2>/dev/null; then ok "server survived all injected faults"
else bad "server died during fault injection"; fi
stop_server

hdr "Case 2 vs Case 1 - redundancy under an identical fault pattern"
# Same topic, same p, same seed, both cases.  This is the headline comparison
# of Part II, so it is asserted here as well as measured by the benchmark.
C1="$HERE/../src/restart"
( cd "$C1" && make >/dev/null 2>&1 )
SEED=31337
P=0.004
start_server "$P" --quiet --seed "$SEED" || exit 1
"$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/cmp_c2" --quiet > "$WORK/cmp_c2.out" 2>&1
c2_rc=$?
stop_server
PORT=$(free_port 30900)
"$C1/server" "$PORT" "$ROOT" "$P" --quiet --seed "$SEED" > "$WORK/cmp_c1_srv.log" 2>&1 &
SRV_PID=$!
wait_port "$PORT" || bad "Case 1 server failed to listen"
"$C1/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/cmp_c1" --quiet > "$WORK/cmp_c1.out" 2>&1
c1_rc=$?
stop_server

check_eq "Case 2 comparison run succeeded" "0" "$c2_rc"
check_eq "Case 1 comparison run succeeded" "0" "$c1_rc"
r1=$(result_field "$WORK/cmp_c1.out" redundant_bytes)
r2=$(result_field "$WORK/cmp_c2.out" redundant_bytes)
f1=$(result_field "$WORK/cmp_c1.out" reconnects)
f2=$(result_field "$WORK/cmp_c2.out" reconnects)
note "Case 1: $f1 fault(s), $r1 redundant bytes"
note "Case 2: $f2 fault(s), $r2 redundant bytes"
if [ "${f2:-0}" -gt 0 ] && [ "${r2:-0}" -lt "${r1:-1}" ]; then
    ok "session management cut redundant bytes from $r1 to $r2"
elif [ "${f2:-0}" -eq 0 ]; then
    note "no faults fired in the Case 2 run, so there is nothing to compare"
else
    bad "Case 2 redundancy ($r2) is not below Case 1 ($r1)"
fi

summary "PART 2 CASE 2"
