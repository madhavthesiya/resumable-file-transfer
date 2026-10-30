#!/bin/bash
# Functional tests for baseline: correctness of the transfer in both threading
# modes, error handling, and byte-identical output under concurrency.
#
#   ./test_baseline.sh [topics_root] [work_dir]
#
# Exits 0 only if every check passes.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"

ROOT=${1:-$HERE/../.dataset/synthetic}
WORK=${2:-$HERE/../.work/baseline}
BIN="$HERE/../src/baseline"

rm -rf "$WORK"; mkdir -p "$WORK"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

start_server() {                       # start_server <mode>
    PORT=$(free_port 29100)
    "$BIN/server" "$PORT" "$ROOT" "$1" --quiet > "$WORK/server_$1.log" 2>&1 &
    SRV_PID=$!
    if ! wait_port "$PORT"; then
        bad "server ($1) failed to listen; log follows"; cat "$WORK/server_$1.log"; return 1
    fi
    note "server ($1) listening on port $PORT (pid $SRV_PID)"
}
stop_server() { kill "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""; }

hdr "Part 1 - build"
( cd "$BIN" && make clean >/dev/null && make > "$WORK/build.log" 2>&1 )
if [ -x "$BIN/server" ] && [ -x "$BIN/client" ]; then
    ok "make produced ./server and ./client"
else
    bad "make did not produce both executables"; cat "$WORK/build.log"; exit 1
fi
# Count real ELF binaries rather than trusting the executable bit: files on a
# Windows drive mounted into WSL all report mode 777, so -executable would match
# the sources too.  What the grader cares about is that exactly server and
# client are produced, and nothing else.
elf_count=0
for f in "$BIN"/*; do
    [ -f "$f" ] || continue
    if [ "$(head -c4 "$f" | od -An -tx1 | tr -d "[:space:]")" = "7f454c46" ]; then
        elf_count=$((elf_count+1))
    fi
done
check_eq "make produced exactly 2 ELF executables" "2" "$elf_count"

# ---------------------------------------------------------------- multi mode
hdr "Part 1 - multi-threaded, single client, every topic"
start_server multi || exit 1
for topic in $(ls "$ROOT"); do
    out="$WORK/multi_one"
    "$BIN/client" 127.0.0.1 "$PORT" "$topic" "$out" --quiet > "$WORK/c_$topic.out" 2>"$WORK/c_$topic.err"
    rc=$?
    check_eq "client exit status for '$topic'" "0" "$rc"
    check_eq "RESULT status for '$topic'" "ok" "$(result_field "$WORK/c_$topic.out" status)"
    nf=$(ls -1 "$ROOT/$topic" | wc -l)
    check_eq "file count for '$topic'" "$nf" "$(result_field "$WORK/c_$topic.out" files)"
    check_eq "crc failures for '$topic'" "0" "$(result_field "$WORK/c_$topic.out" crc_fail)"
    compare_trees "$ROOT/$topic" "$out/$topic" "topic '$topic'"
done

hdr "Part 1 - error handling (must not crash)"
"$BIN/client" 127.0.0.1 "$PORT" no_such_topic "$WORK/err" --quiet >"$WORK/err.out" 2>"$WORK/err.err"
check_eq "unknown topic exits non-zero" "1" "$?"
if grep -q "ERR_UNKNOWN_TOPIC" "$WORK/err.err"; then
    ok "unknown topic reported as ERR_UNKNOWN_TOPIC"
else
    bad "unknown topic did not report ERR_UNKNOWN_TOPIC"; head -3 "$WORK/err.err"
fi
if kill -0 "$SRV_PID" 2>/dev/null; then
    ok "server still alive after a bad request"
else
    bad "server died after a bad request"
fi
# A path-traversal attempt must be refused by the client's own argument check.
"$BIN/client" 127.0.0.1 "$PORT" "../../etc" "$WORK/err" --quiet >/dev/null 2>&1
check_eq "traversal topic name refused" "2" "$?"
if kill -0 "$SRV_PID" 2>/dev/null; then ok "server unaffected by traversal attempt"
else bad "server died on traversal attempt"; fi
stop_server

# ------------------------------------------------------------- concurrency
# Runs N clients at once, each into its own output directory, then verifies
# every one of them byte for byte.  This is the check that the multi-threaded
# server does not interleave or corrupt data across workers.
concurrent_test() {                    # concurrent_test <mode> <n> <topic>
    local mode=$1 n=$2 topic=$3
    local base="$WORK/conc_${mode}_$n"
    rm -rf "$base"; mkdir -p "$base"
    local pids=() i
    local t0 t1
    t0=$(date +%s.%N)
    for ((i = 0; i < n; i++)); do
        "$BIN/client" 127.0.0.1 "$PORT" "$topic" "$base/c$i" --quiet \
            > "$base/c$i.out" 2> "$base/c$i.err" &
        pids+=($!)
    done
    local allok=1
    for p in "${pids[@]}"; do wait "$p" || allok=0; done
    t1=$(date +%s.%N)

    if [ $allok -eq 1 ]; then ok "$mode/$n clients: all exited 0"
    else bad "$mode/$n clients: at least one failed"; head -5 "$base"/c*.err; fi

    local bad_trees=0
    for ((i = 0; i < n; i++)); do
        diff -r "$ROOT/$topic" "$base/c$i/$topic" >/dev/null 2>&1 || bad_trees=$((bad_trees+1))
    done
    check_eq "$mode/$n clients: every output tree is byte-identical" "0" "$bad_trees"

    # Aggregate throughput, as Part I asks for: total useful bytes across all
    # clients divided by the wall-clock span of the whole batch.
    local total=0 b
    for ((i = 0; i < n; i++)); do
        b=$(result_field "$base/c$i.out" useful_bytes); total=$((total + ${b:-0}))
    done
    local secs mbps
    secs=$(echo "$t1 $t0" | awk '{printf "%.4f", $1-$2}')
    mbps=$(echo "$total $secs" | awk '{printf "%.1f", $1*8/$2/1e6}')
    note "$mode/$n clients: $total useful bytes in ${secs}s = ${mbps} Mbit/s aggregate"
    echo "$mode,$n,$total,$secs,$mbps" >> "$WORK/throughput.csv"
}

hdr "Part 1 - multi-threaded concurrency (1, 2, 4, 8 clients)"
start_server multi || exit 1
for n in 1 2 4 8; do concurrent_test multi "$n" topic_beta; done
stop_server

hdr "Part 1 - single-threaded mode"
start_server single || exit 1
out="$WORK/single_one"
"$BIN/client" 127.0.0.1 "$PORT" topic_alpha "$out" --quiet >"$WORK/s1.out" 2>&1
check_eq "single-threaded client exit status" "0" "$?"
check_eq "single-threaded RESULT status" "ok" "$(result_field "$WORK/s1.out" status)"
compare_trees "$ROOT/topic_alpha" "$out/topic_alpha" "single-threaded topic_alpha"

hdr "Part 1 - single-threaded concurrency (clients are served sequentially)"
for n in 1 2 4 8; do concurrent_test single "$n" topic_beta; done
stop_server

hdr "Part 1 - boundary cases"
start_server multi || exit 1
# The empty file must arrive as a zero-length file, not be skipped.
out="$WORK/empty"
"$BIN/client" 127.0.0.1 "$PORT" topic_alpha "$out" --quiet >/dev/null 2>&1
empties=$(find "$out/topic_alpha" -type f -size 0 | wc -l)
check_eq "the zero-length file was created" "1" "$empties"
# The 5 MB file must match exactly (it spans many chunks).
big_src=$(find "$ROOT/topic_alpha" -type f -size +4M | head -1)
big_dst="$out/topic_alpha/$(basename "$big_src")"
check_eq "5 MB file sha256 matches" "$(sha256sum "$big_src" | cut -d' ' -f1)" \
                                    "$(sha256sum "$big_dst" | cut -d' ' -f1)"

# A non-default chunk size must not change the bytes delivered.
stop_server
PORT=$(free_port 29200)
"$BIN/server" "$PORT" "$ROOT" multi --chunk 1024 --quiet > "$WORK/server_chunk.log" 2>&1 &
SRV_PID=$!
wait_port "$PORT" || bad "server with --chunk 1024 failed to listen"
"$BIN/client" 127.0.0.1 "$PORT" topic_gamma "$WORK/chunk1k" --quiet >/dev/null 2>&1
check_eq "client exit with --chunk 1024" "0" "$?"
compare_trees "$ROOT/topic_gamma" "$WORK/chunk1k/topic_gamma" "chunk=1024 topic_gamma"
stop_server

hdr "Part 1 - an empty topic directory is served as zero files"
EDGE=$HERE/../.dataset/synthetic_edge
if [ -d "$EDGE" ]; then
    PORT=$(free_port 29300)
    "$BIN/server" "$PORT" "$EDGE" multi --quiet > "$WORK/server_edge.log" 2>&1 &
    SRV_PID=$!
    wait_port "$PORT" || bad "edge server failed to listen"
    "$BIN/client" 127.0.0.1 "$PORT" topic_empty "$WORK/edge" --quiet >"$WORK/edge.out" 2>&1
    check_eq "empty topic exits 0" "0" "$?"
    check_eq "empty topic reports 0 files" "0" "$(result_field "$WORK/edge.out" files)"
    "$BIN/client" 127.0.0.1 "$PORT" topic_one "$WORK/edge" --quiet >"$WORK/edge1.out" 2>&1
    check_eq "single-file topic exits 0" "0" "$?"
    check_eq "single-file topic reports 1 file" "1" "$(result_field "$WORK/edge1.out" files)"
    stop_server
else
    note "skipping: $EDGE not present"
fi

hdr "Part 1 - graceful shutdown on SIGINT"
# Started without --quiet: the drain messages are logged at INFO level.
PORT=$(free_port 29400)
"$BIN/server" "$PORT" "$ROOT" multi > "$WORK/server_shutdown.log" 2>&1 &
SRV_PID=$!
wait_port "$PORT" || bad "shutdown-test server failed to listen"
kill -INT "$SRV_PID"
sleep 0.6
if kill -0 "$SRV_PID" 2>/dev/null; then
    bad "server still running 0.6 s after SIGINT"; kill -9 "$SRV_PID" 2>/dev/null
else
    ok "server exited on SIGINT"
fi
wait "$SRV_PID" 2>/dev/null
rc=$?
check_eq "SIGINT exit status is 0" "0" "$rc"
if grep -q "all workers finished" "$WORK/server_shutdown.log"; then
    ok "shutdown path logged a clean worker drain"
else
    bad "shutdown did not log a clean worker drain"; tail -3 "$WORK/server_shutdown.log"
fi
SRV_PID=""

summary "PART 1"
