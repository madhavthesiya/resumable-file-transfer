#!/bin/bash
# Runs one Part under AddressSanitizer+UBSan, ThreadSanitizer and valgrind.
#
#   ./test_sanitizers.sh <PartDir> [topics_root] [topic] [server_extra_args...]
#
# Example:
#   ./test_sanitizers.sh baseline                                  # defaults
#   ./test_sanitizers.sh restart "$HERE/../.dataset/synthetic" topic_alpha 0.0005
#
# Deliberately small runs: the point is to exercise every code path (including
# the fault/resume paths) under instrumentation, not to measure speed.

set -u
HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/lib.sh"

PART=${1:-baseline}
ROOT=${2:-$HERE/../.dataset/synthetic}
TOPIC=${3:-topic_alpha}
shift 3 2>/dev/null || shift $# 
SRV_EXTRA=("$@")

DIR="$HERE/../src/$PART"
WORK=$HERE/../.work/san_$PART
rm -rf "$WORK"; mkdir -p "$WORK"

SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null; wait 2>/dev/null; }
trap cleanup EXIT

# Markers every sanitizer prints; valgrind's own summary is checked separately.
SAN_RE='ERROR: AddressSanitizer|WARNING: ThreadSanitizer|runtime error:|SUMMARY: (Address|Undefined|Thread)Sanitizer|LeakSanitizer'

scan() {                               # scan <label> <logfile>
    if grep -qE "$SAN_RE" "$2"; then
        bad "$1: sanitizer reported a problem"
        grep -nE "$SAN_RE" "$2" | head -5
        sed -n '1,25p' "$2"
    else
        ok "$1: no sanitizer findings"
    fi
}

run_mode() {                           # run_mode <asan|tsan> <nclients>
    local mode=$1 n=$2
    hdr "$PART - $mode ($n client(s), topic $TOPIC)"
    ( cd "$DIR" && make "$mode" ) > "$WORK/build_$mode.log" 2>&1 || {
        bad "$mode build failed"; tail -5 "$WORK/build_$mode.log"; return 1; }

    local pre=()
    # ThreadSanitizer aborts on WSL2's address-space randomisation; setarch -R
    # disables ASLR for the child only.
    [ "$mode" = tsan ] && command -v setarch >/dev/null 2>&1 && pre=(setarch "$(uname -m)" -R)

    local PORT; PORT=$(free_port 29800)
    export ASAN_OPTIONS="detect_leaks=1:halt_on_error=0:log_path=$WORK/asan_srv"
    export TSAN_OPTIONS="halt_on_error=0:log_path=$WORK/tsan_srv"
    "${pre[@]}" "$DIR/build/server_$mode" "$PORT" "$ROOT" "${SRV_EXTRA[@]}" \
        > "$WORK/srv_$mode.log" 2>&1 &
    SRV_PID=$!
    if ! wait_port "$PORT"; then
        bad "$mode server failed to listen"; tail -20 "$WORK/srv_$mode.log"; return 1
    fi

    local i pids=()
    for ((i = 0; i < n; i++)); do
        ASAN_OPTIONS="detect_leaks=1:halt_on_error=0" TSAN_OPTIONS="halt_on_error=0" \
        "${pre[@]}" "$DIR/build/client_$mode" 127.0.0.1 "$PORT" "$TOPIC" \
            "$WORK/${mode}_out$i" --quiet > "$WORK/cli_${mode}_$i.log" 2>&1 &
        pids+=($!)
    done
    local allok=1
    for p in "${pids[@]}"; do wait "$p" || allok=0; done
    if [ $allok -eq 1 ]; then ok "$mode: all $n client(s) exited 0"
    else bad "$mode: a client exited non-zero"; tail -15 "$WORK/cli_${mode}_0.log"; fi

    # Data must still be correct under instrumentation.
    local badtrees=0
    for ((i = 0; i < n; i++)); do
        diff -r "$ROOT/$TOPIC" "$WORK/${mode}_out$i/$TOPIC" >/dev/null 2>&1 || badtrees=$((badtrees+1))
    done
    check_eq "$mode: output byte-identical for all clients" "0" "$badtrees"

    kill -INT "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""
    unset ASAN_OPTIONS TSAN_OPTIONS

    for f in "$WORK/srv_$mode.log" "$WORK"/cli_${mode}_*.log; do
        [ -f "$f" ] && scan "$mode $(basename "$f")" "$f"
    done
    # Sanitizers write to log_path.<pid> when it is set.
    for f in "$WORK/${mode}_srv".*; do [ -f "$f" ] && scan "$mode $(basename "$f")" "$f"; done
    return 0
}

run_mode asan 2
run_mode tsan 4

# ------------------------------------------------------------------ valgrind
hdr "$PART - valgrind memcheck (server and client)"
VG=""
if command -v valgrind >/dev/null 2>&1; then
    VG=valgrind
elif [ -x "$HERE/../.work/local/usr/bin/valgrind" ]; then
    # Locally unpacked valgrind needs to be told where its own tools live.
    export VALGRIND_LIB="$HERE/../.work/local/usr/libexec/valgrind"
    VG="$HERE/../.work/local/usr/bin/valgrind"
fi

if [ -z "$VG" ]; then
    note "valgrind not available; skipping"
else
    ( cd "$DIR" && make debug ) > "$WORK/build_dbg.log" 2>&1 || {
        bad "debug build failed"; tail -5 "$WORK/build_dbg.log"; }

    VGOPTS=(--leak-check=full --show-leak-kinds=all --errors-for-leak-kinds=definite
            --track-origins=yes --error-exitcode=99 --num-callers=20)
    PORT=$(free_port 29900)
    "$VG" "${VGOPTS[@]}" --log-file="$WORK/vg_server.log" \
        "$DIR/build/server_dbg" "$PORT" "$ROOT" "${SRV_EXTRA[@]}" \
        > "$WORK/vg_srv_stdout.log" 2>&1 &
    SRV_PID=$!
    # valgrind slows start-up (the checksum pass especially), so allow longer.
    i=0; while [ $i -lt 600 ]; do ss -ltn 2>/dev/null | grep -q ":$PORT " && break; sleep 0.1; i=$((i+1)); done

    if ! ss -ltn 2>/dev/null | grep -q ":$PORT "; then
        bad "valgrind server failed to listen"; tail -20 "$WORK/vg_server.log"
    else
        "$VG" "${VGOPTS[@]}" --log-file="$WORK/vg_client.log" \
            "$DIR/build/client_dbg" 127.0.0.1 "$PORT" "$TOPIC" "$WORK/vg_out" --quiet \
            > "$WORK/vg_cli_stdout.log" 2>&1
        check_eq "client under valgrind exited 0" "0" "$?"
        if diff -r "$ROOT/$TOPIC" "$WORK/vg_out/$TOPIC" >/dev/null 2>&1; then
            ok "valgrind run produced byte-identical output"
        else
            bad "valgrind run produced different output"
        fi
    fi
    kill -INT "$SRV_PID" 2>/dev/null
    # Give valgrind time to write its leak report before the process is reaped.
    i=0; while kill -0 "$SRV_PID" 2>/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
    kill -9 "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""

    for who in server client; do
        L="$WORK/vg_$who.log"
        [ -f "$L" ] || { bad "valgrind $who log missing"; continue; }
        errs=$(sed -n 's/.*ERROR SUMMARY: \([0-9]*\) errors.*/\1/p' "$L" | tail -1)
        lost=$(sed -n 's/.*definitely lost: \([0-9,]*\) bytes.*/\1/p' "$L" | tail -1 | tr -d ,)
        check_eq "valgrind $who: 0 errors" "0" "${errs:-missing}"
        check_eq "valgrind $who: 0 bytes definitely lost" "0" "${lost:-0}"
        if [ "${errs:-1}" != "0" ]; then sed -n '1,60p' "$L"; fi
    done
fi

summary "SANITIZERS/$PART"
