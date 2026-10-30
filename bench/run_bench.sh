#!/bin/bash
# run_bench.sh - the measurement harness for the whole project.
#
#   ./run_bench.sh [options]
#
#     --data <dir>     topics root            (default ~/it305/data/raw-img)
#     --out  <dir>     where CSVs are written (default ../Results)
#     --work <dir>     scratch for client output (default ~/it305/work/bench)
#     --only <list>    comma-separated experiment ids to run, e.g. --only e1,e3
#     --repeats <n>    repeats for the stochastic (fault) experiments (default 3)
#     --quick          a much smaller matrix, for a smoke test of the harness
#     --keep-output    do NOT delete each run's received files
#     --no-warm        skip the page-cache warm-up
#
# WHY OUTPUT IS DELETED BY DEFAULT
# --------------------------------
# Every run writes a full copy of the topic it fetched.  The matrix below
# transfers well over 20 GB in total, and on the development machine the WSL
# ext4 volume is a sparse file on a nearly full Windows disk, so retaining all
# of it is not possible.  Each run is therefore verified (status=ok and
# crc_fail=0, which is what actually establishes correctness) and its output
# tree is then removed before the next run starts.  Peak usage stays at roughly
# one run.  Pass --keep-output to retain everything instead.
#
# p IS PER CHUNK, NOT PER TRANSFER
# --------------------------------
# The server draws once per emitted 16 KiB chunk, so the expected number of
# faults in one pass is p * chunks.  Chunk counts for the real dataset:
# scoiattolo 1551, cane 3705, gatto 9226.  The p values used below were picked
# from those counts to give a useful range of faults per pass rather than round
# numbers; see Results/SUMMARY.md.

set -u

HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/bench_lib.sh"
SUB=$(cd "$HERE/.." && pwd)

DATA=${HOME}/it305/data/raw-img
OUT="$SUB/Results"
WORK=${HOME}/it305/work/bench
ONLY=""
REPEATS=3
QUICK=0
KEEP=0
export NO_WARM=${NO_WARM:-0}

while [ $# -gt 0 ]; do
    case $1 in
        --data)        DATA=$2; shift 2 ;;
        --out)         OUT=$2; shift 2 ;;
        --work)        WORK=$2; shift 2 ;;
        --only)        ONLY=$2; shift 2 ;;
        --repeats)     REPEATS=$2; shift 2 ;;
        --quick)       QUICK=1; shift ;;
        --keep-output) KEEP=1; shift ;;
        --no-warm)     export NO_WARM=1; shift ;;
        -h|--help)     sed -n '2,40p' "$0"; exit 0 ;;
        *)             die "unknown option '$1'" ;;
    esac
done

[ -d "$DATA" ] || die "topics root '$DATA' does not exist"
mkdir -p "$OUT" "$WORK"

SUMMARY="$OUT/bench_runs.csv"
if [ ! -f "$SUMMARY" ]; then
    echo "exp,case,topic,mode,p,K,clients,repeat,seed,status,files,useful_bytes,redundant_bytes,wire_rx_bytes,makespan_s,wall_span_s,max_client_elapsed_s,agg_goodput_mbps,overhead_pct,reconnects,retries,crc_ok,crc_fail" > "$SUMMARY"
fi

wanted() {                               # wanted <exp-id>
    [ -z "$ONLY" ] && return 0
    case ",$ONLY," in *",$1,"*) return 0 ;; *) return 1 ;; esac
}

# ---------------------------------------------------------------- one data point
# measure <exp> <partdir> <case> <topic> <clients> <repeat> <seed> \
#         <srv_pos_arg> <mode-label> <p-label> <K-label> [client extra args...]
#
# srv_pos_arg is the server's optional third positional: "single"/"multi" for
# baseline, the failure probability for the Part2 servers.
measure() {
    local exp=$1 part=$2 case_id=$3 topic=$4 n=$5 rep=$6 seed=$7 srvarg=$8
    local mode=$9 plab=${10} klab=${11}
    shift 11
    local bindir="$SUB/$part"
    local runroot="$WORK/${exp}_${case_id}_${topic}_${mode}_${plab}_k${klab}_n${n}_r${rep}"
    local port pid i res pids=() ress=()

    rm -rf -- "${runroot:?runroot is empty}"
    mkdir -p "$runroot"

    port=$(free_port 33000)
    local srv_extra=(--quiet)
    [ "$case_id" != "baseline" ] && srv_extra+=(--seed "$seed")
    "$bindir/server" "$port" "$DATA" "$srvarg" "${srv_extra[@]}" \
        > "$runroot/server.log" 2>&1 &
    local srv=$!
    if ! wait_port "$port"; then
        msg "$exp: server failed to listen; log follows"
        sed -n '1,10p' "$runroot/server.log"
        kill -9 "$srv" 2>/dev/null; wait "$srv" 2>/dev/null
        rm -rf -- "${runroot:?}"
        return 1
    fi

    local t0 t1
    t0=$(date +%s.%N)
    for i in $(seq 1 "$n"); do
        res="$runroot/c$i.out"
        ress+=("$res")
        launch_client "$i" localhost "$bindir" 127.0.0.1 "$port" \
                      "$topic" "$runroot" "$res" "$@"
        pids+=("$LAST_CLIENT_PID")
    done
    local bad_exit=0
    for pid in "${pids[@]}"; do wait "$pid" || bad_exit=1; done
    t1=$(date +%s.%N)

    kill "$srv" 2>/dev/null; wait "$srv" 2>/dev/null

    # ---- aggregate the per-client RESULT lines
    local makespan wall maxel useful redundant wire_rx files crc_ok crc_fail recon retries
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.6f", b-a}')
    maxel=$(max_field_f elapsed_s "${ress[@]}")

    # `date` reads CLOCK_REALTIME, which under WSL2 is resynchronised against
    # the host and can step backwards mid-run; an early validation pass
    # recorded a wall span of -0.42 s this way.  The span is therefore only
    # trusted when it is positive and at least as large as the longest
    # client-measured transfer, which it must be by construction.  Both raw
    # figures are kept in the CSV so any such substitution stays visible.
    makespan=$(awk -v w="$wall" -v e="$maxel" \
        'BEGIN{printf "%.6f", (w > 0 && w >= e) ? w : e}')
    if awk -v w="$wall" -v e="$maxel" 'BEGIN{exit !(w <= 0 || w < e)}'; then
        msg "clock anomaly: wall span ${wall}s vs longest client ${maxel}s; using the latter"
    fi
    useful=$(sum_field useful_bytes "${ress[@]}")
    redundant=$(sum_field redundant_bytes "${ress[@]}")
    wire_rx=$(sum_field wire_rx_bytes "${ress[@]}")
    files=$(sum_field files "${ress[@]}")
    crc_ok=$(sum_field crc_ok "${ress[@]}")
    crc_fail=$(sum_field crc_fail "${ress[@]}")
    recon=$(sum_field reconnects "${ress[@]}")
    retries=$(sum_field retries "${ress[@]}")

    local status=ok
    [ "$bad_exit" -ne 0 ] && status=client_exit
    [ "$crc_fail" -ne 0 ] && status=crc_fail
    for res in "${ress[@]}"; do
        grep -q "status=ok" "$res" 2>/dev/null || status=incomplete
    done

    local goodput overhead
    goodput=$(awk -v b="$useful" -v s="$makespan" 'BEGIN{printf "%.3f", (s>0)? b*8/s/1e6 : 0}')
    overhead=$(awk -v w="$wire_rx" -v u="$useful" 'BEGIN{printf "%.4f", (u>0)? (w-u)*100.0/u : 0}')

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$exp" "$case_id" "$topic" "$mode" "$plab" "$klab" "$n" "$rep" "$seed" \
        "$status" "$files" "$useful" "$redundant" "$wire_rx" "$makespan" \
        "$wall" "$maxel" "$goodput" "$overhead" "$recon" "$retries" \
        "$crc_ok" "$crc_fail" \
        >> "$SUMMARY"

    printf '  %-5s %-22s %-11s mode=%-6s p=%-7s K=%-2s N=%-2s r=%s  %7.2fs %9.1f Mb/s  redund=%-10s %s\n' \
        "$exp" "$case_id" "$topic" "$mode" "$plab" "$klab" "$n" "$rep" \
        "$makespan" "$goodput" "$redundant" "$status"

    if [ "$KEEP" -eq 0 ]; then
        rm -rf -- "${runroot:?}"
    fi
    [ "$status" = ok ]
}

# ---------------------------------------------------------------- the matrix
FAILED=0
note_fail() { FAILED=$((FAILED+1)); }

# CHOOSING p SO THAT CASE 1 CAN STILL FINISH
# ------------------------------------------
# Case 1 has no checkpoint, so a fault costs the whole topic: the client starts
# the pass again from the first file.  A pass over C chunks survives with
# probability (1-p)^C, so the expected number of passes is (1-p)^-C and the
# expected bytes on the wire are that multiple of the topic size.  For
# scoiattolo (C=1551):
#
#     p=0.0002 -> 1.4 passes      p=0.001 -> 4.7 passes
#     p=0.0005 -> 2.2 passes      p=0.002 -> 22  passes
#     p=0.005  -> 2300 passes  (~55 GB, and past the 500-reconnect cap)
#
# so the sweep stops at 0.002.  gatto has C=9226, where even p=0.001 needs
# ~10000 passes, so the large-file comparison uses p=0.0001 (2.5 passes).
# Case 2 and Enhanced resume from a checkpoint and are not subject to this,
# which is the whole point of the comparison.
if [ "$QUICK" -eq 1 ]; then
    SCALE_TOPIC=scoiattolo;  SCALE_N="1 2"
    SIZE_TOPICS="scoiattolo"
    FAULT_TOPIC=scoiattolo;  FAULT_PS="0 0.001"
    BIG_TOPIC=scoiattolo;    BIG_P=0.001
    K_TOPIC=scoiattolo;      K_LIST="1 4"
    ENH_P=0.001
    REPEATS=1
else
    SCALE_TOPIC=cane;        SCALE_N="1 2 4 8"
    SIZE_TOPICS="scoiattolo cane gatto"
    FAULT_TOPIC=scoiattolo;  FAULT_PS="0 0.0002 0.0005 0.001 0.002"
    BIG_TOPIC=gatto;         BIG_P=0.0001
    K_TOPIC=cane;            K_LIST="1 2 4 8"
    ENH_P=0.001
fi

msg "data root : $DATA"
msg "summary   : $SUMMARY"
msg "output    : $([ "$KEEP" -eq 1 ] && echo retained || echo "deleted after each run")"

# --- E1: Part 1, single vs multi-threaded server, N concurrent clients
if wanted e1; then
    echo; msg "E1  baseline: threading mode x concurrent clients (topic $SCALE_TOPIC)"
    warm_cache "$DATA" "$SCALE_TOPIC"
    for mode in single multi; do
        for n in $SCALE_N; do
            for r in $(seq 1 "$REPEATS"); do
                measure e1 baseline baseline "$SCALE_TOPIC" "$n" "$r" 0 "$mode" "$mode" 0 1 || note_fail
            done
        done
    done
fi

# --- E2: Part 1, goodput against topic size
if wanted e2; then
    echo; msg "E2  baseline: goodput vs topic size (multi, 1 client)"
    for t in $SIZE_TOPICS; do
        warm_cache "$DATA" "$t"
        for r in $(seq 1 "$REPEATS"); do
            measure e2 baseline baseline "$t" 1 "$r" 0 multi multi 0 1 || note_fail
        done
    done
fi

# --- E3: Case 1 vs Case 2 across the fault rate
if wanted e3; then
    echo; msg "E3  Case1 vs Case2: fault-rate sweep (topic $FAULT_TOPIC, $REPEATS repeats)"
    warm_cache "$DATA" "$FAULT_TOPIC"
    for p in $FAULT_PS; do
        for r in $(seq 1 "$REPEATS"); do
            measure e3 restart restart "$FAULT_TOPIC" 1 "$r" "$((1000+r))" \
                    "$p" faults "$p" 1 || note_fail
            measure e3 resume resume "$FAULT_TOPIC" 1 "$r" "$((1000+r))" \
                    "$p" resume "$p" 1 || note_fail
        done
    done
fi

# --- E4: the same comparison on a large-file topic, where restart hurts most
if wanted e4; then
    echo; msg "E4  Case1 vs Case2 on large files (topic $BIG_TOPIC, p=$BIG_P)"
    warm_cache "$DATA" "$BIG_TOPIC"
    for r in $(seq 1 "$REPEATS"); do
        measure e4 restart restart "$BIG_TOPIC" 1 "$r" "$((2000+r))" \
                "$BIG_P" faults "$BIG_P" 1 || note_fail
        measure e4 resume resume "$BIG_TOPIC" 1 "$r" "$((2000+r))" \
                "$BIG_P" resume "$BIG_P" 1 || note_fail
    done
fi

# --- E5: Enhanced, K parallel connections, fault-free
if wanted e5; then
    echo; msg "E5  Enhanced: K parallel connections, fault-free (topic $K_TOPIC)"
    warm_cache "$DATA" "$K_TOPIC"
    for k in $K_LIST; do
        for r in $(seq 1 "$REPEATS"); do
            measure e5 parallel parallel "$K_TOPIC" 1 "$r" 0 \
                    0 enhanced 0 "$k" "$k" || note_fail
        done
    done
fi

# --- E6: Enhanced under faults, K=1 against K=4
if wanted e6; then
    echo; msg "E6  Enhanced under faults (topic $K_TOPIC, p=$ENH_P)"
    warm_cache "$DATA" "$K_TOPIC"
    for k in 1 4; do
        for r in $(seq 1 "$REPEATS"); do
            measure e6 parallel parallel "$K_TOPIC" 1 "$r" \
                    "$((3000+r))" "$ENH_P" enhanced "$ENH_P" "$k" "$k" || note_fail
        done
    done
fi

# --- E7: the fault-free comparison, every implementation on ONE topic
# The other experiments use whichever topic suits what they measure, so their
# p=0 rows sit on different topics and cannot be divided by one another.  This
# experiment exists so "compare your results with Part 1 values" has a like-for-
# like basis: same topic, same client count, faults disabled everywhere.
if wanted e7; then
    echo; msg "E7  fault-free comparison across all implementations (topic $FAULT_TOPIC)"
    warm_cache "$DATA" "$FAULT_TOPIC"
    for r in $(seq 1 "$REPEATS"); do
        measure e7 baseline baseline "$FAULT_TOPIC" 1 "$r" 0 \
                multi multi 0 1 || note_fail
        measure e7 restart restart "$FAULT_TOPIC" 1 "$r" 0 \
                0 faults 0 1 || note_fail
        measure e7 resume resume "$FAULT_TOPIC" 1 "$r" 0 \
                0 resume 0 1 || note_fail
        measure e7 parallel parallel "$FAULT_TOPIC" 1 "$r" 0 \
                0 enhanced 0 1 1 || note_fail
        measure e7 parallel parallel "$FAULT_TOPIC" 1 "$r" 0 \
                0 enhanced 0 4 4 || note_fail
    done
fi

echo
if [ "$FAILED" -eq 0 ]; then
    msg "every run completed with status=ok and crc_fail=0"
else
    msg "$FAILED run(s) did not reach status=ok - see $SUMMARY"
fi
msg "rows written to $SUMMARY"
exit $([ "$FAILED" -eq 0 ] && echo 0 || echo 1)
