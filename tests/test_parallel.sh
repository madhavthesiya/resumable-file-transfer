#!/bin/bash
# Functional tests for Part2 Case 2 Enhanced (K parallel connections).
#
#   ./test_parallel.sh [topics_root] [work_dir]
#
# The central claim this file has to prove is the threading one: however many
# connections a client opens, the server must use exactly ONE thread for it.
# That is asserted from the server's own accounting, not assumed.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"

ROOT=${1:-$HERE/../.dataset/synthetic}
WORK=${2:-$HERE/../.work/parallel}
BIN="$HERE/../src/parallel"
TOPIC=topic_alpha

rm -rf "$WORK"; mkdir -p "$WORK"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

# Servers in this file log at INFO level (not --quiet) because several checks
# parse the shutdown summary, which is an INFO record.  Output goes to a file,
# so the extra verbosity costs nothing.
LOGN=0
start_server() {                       # start_server <p> [extra args...]
    local p=$1; shift
    LOGN=$((LOGN+1))
    SRVLOG="$WORK/srv_$LOGN.log"
    PORT=$(free_port 31600)
    "$BIN/server" "$PORT" "$ROOT" "$p" "$@" > "$SRVLOG" 2>&1 &
    SRV_PID=$!
    wait_port "$PORT" || { bad "server (p=$p) failed to listen"; tail -5 "$SRVLOG"; return 1; }
    note "server on $PORT, p=$p, log $(basename "$SRVLOG")"
}
stop_server() { kill -INT "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""; }

# Pull "served by M owner thread(s)" out of the server's shutdown summary.
owner_threads() { sed -n 's/.*served by \([0-9]*\) owner thread.*/\1/p' "$1" | tail -1; }
sockets_accepted() { sed -n 's/.*summary: \([0-9]*\) socket(s) accepted.*/\1/p' "$1" | tail -1; }

hdr "Enhanced - build and CLI contract"
( cd "$BIN" && make clean >/dev/null && make > "$WORK/build.log" 2>&1 )
elf=0
for f in "$BIN"/*; do
    [ -f "$f" ] || continue
    [ "$(head -c4 "$f" | od -An -tx1 | tr -d "[:space:]")" = "7f454c46" ] && elf=$((elf+1))
done
check_eq "make produced exactly 2 ELF executables" "2" "$elf"
"$BIN/server" 31997 "$ROOT" >/dev/null 2>&1
check_eq "server without failure_probability exits 2" "2" "$?"
"$BIN/client" 127.0.0.1 31997 t out 99 >/dev/null 2>&1
check_eq "client rejects K=99" "2" "$?"
"$BIN/client" 127.0.0.1 31997 t out 0 >/dev/null 2>&1
check_eq "client rejects K=0" "2" "$?"

hdr "Enhanced - p=0, every topic, default K"
start_server 0 || exit 1
for topic in $(ls "$ROOT"); do
    "$BIN/client" 127.0.0.1 "$PORT" "$topic" "$WORK/p0" --quiet > "$WORK/p0_$topic.out" 2>&1
    check_eq "$topic: client exit status" "0" "$?"
    check_eq "$topic: status"             "ok" "$(result_field "$WORK/p0_$topic.out" status)"
    check_eq "$topic: redundant bytes"    "0"  "$(result_field "$WORK/p0_$topic.out" redundant_bytes)"
    check_eq "$topic: crc failures"       "0"  "$(result_field "$WORK/p0_$topic.out" crc_fail)"
    compare_trees "$ROOT/$topic" "$WORK/p0/$topic" "$topic output"
    note "$topic: $(result_field "$WORK/p0_$topic.out" items) items, $(result_field "$WORK/p0_$topic.out" split_files) split file(s), $(result_field "$WORK/p0_$topic.out" goodput_mbps) Mbit/s"
done
stop_server
nclients=$(ls "$ROOT" | wc -l)
owners=$(owner_threads "$SRVLOG")
socks=$(sockets_accepted "$SRVLOG")
check_eq "ONE server thread per client (3 clients)" "$nclients" "$owners"
note "$socks sockets were accepted and served by $owners owner thread(s)"
if [ "${socks:-0}" -gt "${owners:-0}" ]; then
    ok "sockets ($socks) outnumber threads ($owners), so multiplexing really happened"
else
    bad "sockets ($socks) did not outnumber threads ($owners)"
fi

hdr "Enhanced - K = 1, 2, 4, 8 all deliver the same bytes"
for K in 1 2 4 8; do
    start_server 0 || exit 1
    out="$WORK/k$K"
    "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$out" "$K" --quiet > "$WORK/k$K.out" 2>&1
    check_eq "K=$K: client exit status" "0" "$?"
    check_eq "K=$K: reported conns"     "$K" "$(result_field "$WORK/k$K.out" conns)"
    check_eq "K=$K: crc failures"       "0"  "$(result_field "$WORK/k$K.out" crc_fail)"
    compare_trees "$ROOT/$TOPIC" "$out/$TOPIC" "K=$K output"
    stop_server
    o=$(owner_threads "$SRVLOG"); s=$(sockets_accepted "$SRVLOG")
    check_eq "K=$K: still exactly one owner thread" "1" "$o"
    note "K=$K: $s socket(s), $o thread, $(result_field "$WORK/k$K.out" items) items, $(result_field "$WORK/k$K.out" goodput_mbps) Mbit/s"
done

hdr "Enhanced - faults are resumed per connection"
for p in 0.002 0.01; do
    start_server "$p" --seed 4242 || exit 1
    out="$WORK/p$p"
    timeout 300 "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$out" --quiet > "$WORK/p$p.out" 2>&1
    rc=$?
    check_eq "p=$p: client exit status" "0" "$rc"
    check_eq "p=$p: status"             "ok" "$(result_field "$WORK/p$p.out" status)"
    check_eq "p=$p: crc failures"       "0"  "$(result_field "$WORK/p$p.out" crc_fail)"
    compare_trees "$ROOT/$TOPIC" "$out/$TOPIC" "p=$p output"
    rec=$(result_field "$WORK/p$p.out" reconnects)
    red=$(result_field "$WORK/p$p.out" redundant_bytes)
    note "p=$p: $rec reconnect(s), $red redundant bytes"
    if [ "${rec:-0}" -gt 0 ]; then
        ok "p=$p: faults occurred and were recovered per connection"
        limit=$(( rec * (65536 + 16384) ))
        if [ "${red:-0}" -le "$limit" ]; then
            ok "p=$p: redundancy $red <= bound $limit"
        else
            bad "p=$p: redundancy $red exceeds the $limit byte bound"
        fi
    else
        note "p=$p: no fault fired this run"
    fi
    stop_server
    check_eq "p=$p: one owner thread despite reconnects" "1" "$(owner_threads "$SRVLOG")"
done

hdr "Enhanced - killing the client mid-transfer and restarting it resumes"
start_server 0 --chunk 512 --sndbuf 4096 || exit 1
KPORT=$PORT
"$BIN/client" 127.0.0.1 "$KPORT" topic_beta "$WORK/kill" --quiet --rcvbuf 4096 \
    > "$WORK/kill1.out" 2> "$WORK/kill1.err" &
CLI=$!
mid=0
for i in $(seq 1 600); do
    if [ -f "$WORK/kill/topic_beta/.checkpoint" ]; then
        moved=$(grep -c '^c[0-9]*=[1-9]' "$WORK/kill/topic_beta/.checkpoint" 2>/dev/null || true)
        [ "${moved:-0}" -ge 1 ] && { mid=1; break; }
    fi
    kill -0 "$CLI" 2>/dev/null || break
    sleep 0.01
done
check_eq "a checkpoint showing real progress appeared" "1" "$mid"
kill -9 "$CLI" 2>/dev/null; wait "$CLI" 2>/dev/null
note "client killed; checkpoint contents:"
sed 's/^/        /' "$WORK/kill/topic_beta/.checkpoint" 2>/dev/null | head -12
ckconns=$(grep -c '^c[0-9]*=' "$WORK/kill/topic_beta/.checkpoint" 2>/dev/null || echo 0)
check_eq "the checkpoint holds one record per connection" "4" "$ckconns"
if grep -q '^crc32=' "$WORK/kill/topic_beta/.checkpoint"; then
    ok "checkpoint carries its CRC line"
else bad "checkpoint has no CRC line"; fi

"$BIN/client" 127.0.0.1 "$KPORT" topic_beta "$WORK/kill" --quiet --rcvbuf 4096 \
    > "$WORK/kill2.out" 2>"$WORK/kill2.err"
check_eq "restarted client exit status" "0" "$?"
check_eq "restarted client status"      "ok" "$(result_field "$WORK/kill2.out" status)"
total=$(cd "$ROOT/topic_beta" && du -cb . | tail -1 | cut -f1)
got2=$(result_field "$WORK/kill2.out" payload_bytes)
if [ "${got2:-0}" -lt "${total:-0}" ]; then
    ok "the restart fetched only $got2 of $total bytes, so it resumed"
else
    bad "the restart fetched $got2 bytes of a $total byte topic: nothing was reused"
fi
compare_trees "$ROOT/topic_beta" "$WORK/kill/topic_beta" "resumed-after-kill output"
stop_server

hdr "Enhanced - a checkpoint describing a different plan is ignored"
start_server 0 || exit 1
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/replan" --quiet >/dev/null 2>&1
# Re-run with a different K: the stored item indices would mean something else,
# so the record must be discarded rather than misapplied.
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/replan" 2 --quiet >/dev/null 2>&1
check_eq "a completed transfer re-runs cleanly with a different K" "0" "$?"
compare_trees "$ROOT/topic_gamma" "$WORK/replan/topic_gamma" "re-run with K=2"

# Now plant a checkpoint whose K disagrees with the run.
rm -rf "$WORK/mismatch"; mkdir -p "$WORK/mismatch/topic_gamma"
body='magic=IT305-ECKPT1\nsession=00000000deadbee2\ntopic=topic_gamma\nfile_count=5\nconns=2\nsplit=524288\nc0=0 0\nc1=0 0\n'
crc=$(printf "$body" | python3 -c 'import sys,zlib;print("%08x"%zlib.crc32(sys.stdin.buffer.read()))')
{ printf "$body"; printf 'crc32=%s\n' "$crc"; } > "$WORK/mismatch/topic_gamma/.checkpoint"
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/mismatch" 4 > "$WORK/mismatch.out" 2>"$WORK/mismatch.err"
check_eq "mismatched-K checkpoint does not break the client" "0" "$?"
if grep -q "different plan" "$WORK/mismatch.err"; then
    ok "the mismatched checkpoint was detected and ignored"
else
    bad "the mismatched checkpoint was not reported"; tail -3 "$WORK/mismatch.err"
fi
compare_trees "$ROOT/topic_gamma" "$WORK/mismatch/topic_gamma" "output after a mismatched checkpoint"

hdr "Enhanced - a corrupt checkpoint is detected by its CRC"
rm -rf "$WORK/corrupt"; mkdir -p "$WORK/corrupt/topic_gamma"
printf 'magic=IT305-ECKPT1\nsession=00000000deadbee3\ntopic=topic_gamma\nfile_count=5\nconns=4\nsplit=524288\nc0=0 0\nc1=0 0\nc2=0 0\nc3=0 0\ncrc32=deadbeef\n' \
    > "$WORK/corrupt/topic_gamma/.checkpoint"
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/corrupt" > "$WORK/corrupt.out" 2>"$WORK/corrupt.err"
check_eq "a corrupt checkpoint does not stop the client" "0" "$?"
if grep -q "failed its CRC check" "$WORK/corrupt.err"; then
    ok "the corrupt checkpoint was detected by its CRC and ignored"
else
    bad "the corrupt checkpoint was not reported"; tail -3 "$WORK/corrupt.err"
fi
compare_trees "$ROOT/topic_gamma" "$WORK/corrupt/topic_gamma" "output after a corrupt checkpoint"

hdr "Enhanced - error handling"
"$BIN/client" 127.0.0.1 "$PORT" no_such_topic "$WORK/err" --quiet >/dev/null 2>"$WORK/err.err"
check_eq "unknown topic exits non-zero" "1" "$?"
if grep -q "ERR_UNKNOWN_TOPIC" "$WORK/err.err"; then
    ok "unknown topic reported as ERR_UNKNOWN_TOPIC"
else bad "unknown topic not reported properly"; head -3 "$WORK/err.err"; fi
if kill -0 "$SRV_PID" 2>/dev/null; then ok "server alive after a bad request"
else bad "server died after a bad request"; fi
"$BIN/client" 127.0.0.1 "$PORT" "../../etc" "$WORK/err" --quiet >/dev/null 2>&1
check_eq "traversal topic name refused" "2" "$?"
stop_server

hdr "Enhanced - concurrent clients, each with its own single thread"
start_server 0.002 --seed 777 || exit 1
pids=(); n=4
for ((i = 0; i < n; i++)); do
    timeout 300 "$BIN/client" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/conc$i" --quiet \
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
stop_server
o=$(owner_threads "$SRVLOG"); s=$(sockets_accepted "$SRVLOG")
note "$n clients opened $s socket(s) in total, served by $o owner thread(s)"
# With faults each client reconnects, so sockets far exceed 4*K; threads must
# still be one per client (plus at most one extra if an owner retired and a
# later reconnect had to start a new one).
if [ "${o:-0}" -ge "$n" ] && [ "${o:-0}" -le $((n * 2)) ]; then
    ok "owner threads ($o) stayed proportional to clients ($n), not to sockets ($s)"
else
    bad "owner thread count $o is not proportional to $n clients"
fi

summary "PART 2 CASE 2 ENHANCED"
