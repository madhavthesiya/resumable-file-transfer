#!/bin/bash
# run_dist.sh - the three-machine Part I experiment.
#
# Run this ON THE SERVER MACHINE. It starts the server locally, launches
# clients over SSH on the other two machines, waits for all of them, and
# records one row per run in the same schema as Results/bench_runs.csv, so
# summarize.py and plot_results.py read it without changes.
#
#   ./run_dist.sh --server-ip 192.168.1.10 \
#                 --data ~/it305/data/raw-img \
#                 --clients madha@192.168.1.11,madha@192.168.1.12 \
#                 --topic cane --repeats 5
#
#     --server-ip   address the CLIENTS must dial (not 127.0.0.1)
#     --data        topics root on this machine
#     --clients     comma-separated ssh destinations, clients are dealt
#                   round-robin across them
#     --topic       topic to fetch (default cane)
#     --counts      client counts to sweep (default "1 2 4 8")
#     --modes       server modes to sweep (default "single multi")
#     --repeats     repeats per configuration (default 5)
#     --out         output directory (default ../Results/distributed)
#     --remote-dir  where to stage the tree on each client (default ~/it305_dist)
#     --skip-prepare  reuse an existing staged tree instead of rsyncing again
#
# WHY SSH CONNECTIONS ARE WARMED FIRST
# ------------------------------------
# The makespan is wall-clock from launching the first client to the last one
# finishing. A cold SSH handshake costs 100-300 ms, which would be charged to
# the transfer and would swamp a run that only takes a second. Every ssh call
# here shares one ControlMaster connection per host, and those connections are
# established - and the binaries paged in - before the clock starts.

set -u

HERE=$(cd "$(dirname "$0")" && pwd)
. "$HERE/bench_lib.sh"
SUB=$(cd "$HERE/.." && pwd)

SERVER_IP=""
DATA="$HERE/../.dataset/demo"
CLIENTS=""
TOPIC=cane
COUNTS="1 2 4 8"
MODES="single multi"
REPEATS=5
OUT="$SUB/Results/distributed"
REMOTE_DIR="it305_dist"
SKIP_PREPARE=0
EXPERIMENT=baseline          # baseline | enhanced
KLIST="1 2 4 8"           # parallel connections, --experiment enhanced
PROB=0                    # failure probability for the Part II server

while [ $# -gt 0 ]; do
    case $1 in
        --server-ip)    SERVER_IP=$2; shift 2 ;;
        --data)         DATA=$2; shift 2 ;;
        --clients)      CLIENTS=$2; shift 2 ;;
        --topic)        TOPIC=$2; shift 2 ;;
        --counts)       COUNTS=$2; shift 2 ;;
        --modes)        MODES=$2; shift 2 ;;
        --repeats)      REPEATS=$2; shift 2 ;;
        --out)          OUT=$2; shift 2 ;;
        --remote-dir)   REMOTE_DIR=$2; shift 2 ;;
        --skip-prepare) SKIP_PREPARE=1; shift ;;
        --experiment)   EXPERIMENT=$2; shift 2 ;;
        --conns)        KLIST=$2; shift 2 ;;
        --prob)         PROB=$2; shift 2 ;;
        -h|--help)      sed -n '2,32p' "$0"; exit 0 ;;
        *)              die "unknown option '$1'" ;;
    esac
done

[ -n "$SERVER_IP" ] || die "--server-ip is required (the address clients dial)"
[ -n "$CLIENTS" ]   || die "--clients is required, e.g. user@hostB,user@hostC"
[ -d "$DATA" ]      || die "topics root '$DATA' not found"
case "$SERVER_IP" in
    127.*|localhost) die "--server-ip must be this machine's LAN address, not loopback" ;;
esac

# Which executable pair this run exercises.
case "$EXPERIMENT" in
    baseline)    PARTDIR=baseline;                 CASE_ID=baseline;                EXPLABEL=d1 ;;
    enhanced) PARTDIR=parallel;  CASE_ID=parallel; EXPLABEL=d2 ;;
    *)        die "--experiment must be 'baseline' or 'enhanced', got '$EXPERIMENT'" ;;
esac

IFS=',' read -r -a HOSTS <<< "$CLIENTS"
[ "${#HOSTS[@]}" -ge 1 ] || die "no client hosts parsed from '$CLIENTS'"

mkdir -p "$OUT"
SUMMARY="$OUT/bench_runs_dist.csv"
SRVCSV="$OUT/server_runs.csv"
SRVLOG="$OUT/server.log"
if [ ! -f "$SUMMARY" ]; then
    echo "exp,case,topic,mode,p,K,clients,repeat,seed,status,files,useful_bytes,redundant_bytes,wire_rx_bytes,makespan_s,wall_span_s,max_client_elapsed_s,agg_goodput_mbps,overhead_pct,reconnects,retries,crc_ok,crc_fail" > "$SUMMARY"
fi

# One shared, persistent SSH connection per host.
CTLDIR=$(mktemp -d "${TMPDIR:-/tmp}/rundist.XXXXXX")
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new
          -o ControlMaster=auto -o "ControlPath=$CTLDIR/%r@%h:%p"
          -o ControlPersist=120)
export SSH_OPTS_STR="${SSH_OPTS[*]}"

cleanup() {
    for h in "${HOSTS[@]}"; do
        ssh "${SSH_OPTS[@]}" -O exit "$h" 2>/dev/null
    done
    rm -rf "$CTLDIR"
}
trap cleanup EXIT

rsh() { ssh -n -T "${SSH_OPTS[@]}" "$1" "$2"; }

msg "server   : $SERVER_IP  (topics root $DATA)"
msg "clients  : ${HOSTS[*]}"
msg "topic    : $TOPIC"
msg "output   : $SUMMARY"

# ------------------------------------------------------------ reachability
for h in "${HOSTS[@]}"; do
    name=$(rsh "$h" 'hostname' 2>/dev/null) \
        || die "cannot ssh to $h - check 'ssh $h hostname' works without a password"
    # `hostname -I` is Linux-only; macOS wants `ipconfig getifaddr`. Try both
    # and do the field-splitting locally to keep the remote quoting simple.
    ip=$(rsh "$h" "hostname -I 2>/dev/null || ipconfig getifaddr en0 2>/dev/null || ipconfig getifaddr en1 2>/dev/null" \
         | awk '{print $1}')
    os=$(rsh "$h" "uname -s" 2>/dev/null)
    msg "  $h is up: hostname=$name os=$os ip=$ip"
done

# ------------------------------------------------------------- staging
if [ "$SKIP_PREPARE" -eq 0 ]; then
    msg "staging the tree on each client (rsync + make)"
    for h in "${HOSTS[@]}"; do
        rsh "$h" "mkdir -p '$REMOTE_DIR'" || die "mkdir failed on $h"
        # Exclude the locally compiled binaries. Shipping this machine's ELF
        # executables to a Mac is worse than useless: rsync -a preserves their
        # timestamps, so they look newer than the sources and `make` decides
        # there is nothing to build, leaving a Linux binary that macOS cannot
        # execute. Every client then fails with no RESULT line at all.
        rsync -az --delete \
              -e "ssh ${SSH_OPTS[*]}" \
              --exclude 'Results/' --exclude 'build/' --exclude '*.o' \
              --exclude 'report/' --exclude '*.pdf' \
              --exclude 'baseline/server' --exclude 'baseline/client' \
              --exclude 'restart/server' --exclude 'restart/client' \
              --exclude 'resume/server' --exclude 'resume/client' \
              --exclude 'parallel/server' \
              --exclude 'parallel/client' \
              --exclude 'tests/test_common*' \
              "$SUB/" "$h:$REMOTE_DIR/" || die "rsync to $h failed"
        # The Makefile builds with -Werror. clang on macOS emits warnings gcc
        # does not, which would turn a cosmetic diagnostic into a build
        # failure, so fall back to building without -Werror before giving up.
        # `make clean` first so the client is always compiled on the machine
        # that will run it, and verify the result actually executes there -
        # `test -x` only checks the permission bit, which a foreign binary
        # also has.
        # Build ONLY the client target. The default target also builds the
        # server, and the Enhanced server uses epoll and eventfd, which are
        # Linux-only - that fails on macOS. The client hosts never run a
        # server, so there is nothing to lose by skipping it.
        rsh "$h" "cd '$REMOTE_DIR' && make -C $PARTDIR clean >/dev/null 2>&1; { make -C $PARTDIR client >/dev/null 2>&1 || make -C $PARTDIR client WARN='-Wall -Wextra' >/dev/null 2>&1; } && ./$PARTDIR/client 2>&1 | grep -q usage" \
            || die "remote build failed on $h - run 'cd $REMOTE_DIR && make -C $PARTDIR client' there to see why"
        msg "  $h ready"
    done
fi

# -------------------------------------------------------------- one data point
measure_dist() {                     # measure_dist <mode> <n> <repeat> [K]
    local mode=$1 n=$2 rep=$3 k=${4:-1}
    local port outroot res i hostidx pids=() ress=()
    local srvarg cli_extra

    # Part 1 takes the threading mode as its third positional; the Part II
    # servers take the failure probability there instead, and the Enhanced
    # client takes K as an extra trailing argument.
    if [ "$EXPERIMENT" = enhanced ]; then
        srvarg="$PROB"; cli_extra="$k"
    else
        srvarg="$mode"; cli_extra=""
    fi

    port=$(free_port 34000)
    outroot="$REMOTE_DIR/out_${mode}_n${n}_k${k}_r${rep}"
    local local_res="$OUT/.res_${mode}_${n}_${k}_${rep}"
    mkdir -p "$local_res"

    "$SUB/$PARTDIR/server" "$port" "$DATA" "$srvarg" --quiet \
        --csv "$SRVCSV" --log "$SRVLOG" >> "$SRVLOG" 2>&1 &
    local srv=$!
    if ! wait_port "$port"; then
        msg "server failed to listen on $port"; kill -9 "$srv" 2>/dev/null; return 1
    fi

    # Warm the control connections and page the client binary in, so the
    # timed section measures the transfer and not ssh setup.
    for h in "${HOSTS[@]}"; do
        rsh "$h" "cd '$REMOTE_DIR' && ./$PARTDIR/client >/dev/null 2>&1; true"
    done

    local t0 t1
    t0=$(date +%s.%N)
    for i in $(seq 1 "$n"); do
        hostidx=$(( (i - 1) % ${#HOSTS[@]} ))
        res="$local_res/c$i.out"
        ress+=("$res")
        # shellcheck disable=SC2086
        launch_client "$i" "${HOSTS[$hostidx]}" "$REMOTE_DIR/$PARTDIR" \
                      "$SERVER_IP" "$port" "$TOPIC" "$outroot" "$res" $cli_extra
        pids+=("$LAST_CLIENT_PID")
    done
    local bad=0
    for p in "${pids[@]}"; do wait "$p" || bad=1; done
    t1=$(date +%s.%N)

    kill "$srv" 2>/dev/null; wait "$srv" 2>/dev/null

    local wall maxel useful wire files crc_ok crc_fail makespan goodput overhead status err
    local redundant recon
    wall=$(awk -v a="$t0" -v b="$t1" 'BEGIN{printf "%.6f", b-a}')
    maxel=$(max_field_f elapsed_s "${ress[@]}")
    makespan=$(awk -v w="$wall" -v e="$maxel" 'BEGIN{printf "%.6f", (w>0 && w>=e)?w:e}')
    useful=$(sum_field useful_bytes "${ress[@]}")
    wire=$(sum_field wire_rx_bytes "${ress[@]}")
    files=$(sum_field files "${ress[@]}")
    crc_ok=$(sum_field crc_ok "${ress[@]}")
    crc_fail=$(sum_field crc_fail "${ress[@]}")
    redundant=$(sum_field redundant_bytes "${ress[@]}")
    recon=$(sum_field reconnects "${ress[@]}")

    status=ok
    [ "$bad" -ne 0 ] && status=client_exit
    [ "$crc_fail" -ne 0 ] && status=crc_fail
    for res in "${ress[@]}"; do
        grep -q "status=ok" "$res" 2>/dev/null || status=incomplete
    done

    goodput=$(awk -v b="$useful" -v s="$makespan" 'BEGIN{printf "%.3f", (s>0)?b*8/s/1e6:0}')
    overhead=$(awk -v w="$wire" -v u="$useful" 'BEGIN{printf "%.4f", (u>0)?(w-u)*100.0/u:0}')

    printf '%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0,%s,%s\n' \
        "$EXPLABEL" "$CASE_ID" "$TOPIC" "$mode" "$PROB" "$k" "$n" "$rep" \
        "$status" "$files" "$useful" "$redundant" "$wire" \
        "$makespan" "$wall" "$maxel" "$goodput" "$overhead" "$recon" \
        "$crc_ok" "$crc_fail" \
        >> "$SUMMARY"

    if [ "$EXPERIMENT" = enhanced ]; then
        printf '  K=%-2s r=%-2s  %7.2fs  %9.1f Mbit/s  crc_ok=%-7s %s\n' \
            "$k" "$rep" "$makespan" "$goodput" "$crc_ok" "$status"
    else
        printf '  %-6s N=%-2s r=%-2s  %7.2fs  %9.1f Mbit/s  crc_ok=%-7s %s\n' \
            "$mode" "$n" "$rep" "$makespan" "$goodput" "$crc_ok" "$status"
    fi

    # A run that produced no RESULT line is almost always the client failing
    # to start on the remote machine, so surface its stderr rather than
    # leaving a bare "incomplete" to be guessed at.
    if [ "$status" != ok ]; then
        for res in "${ress[@]}"; do
            err="${res%.out}.err"
            if [ -s "$err" ]; then
                echo "      client stderr: $(head -2 "$err" | tr '\n' ' ')"
                break
            fi
        done
    fi

    for h in "${HOSTS[@]}"; do
        rsh "$h" "rm -rf -- '${outroot:?}'" 2>/dev/null
    done
    rm -rf "$local_res"
    [ "$status" = ok ]
}

# ------------------------------------------------------------------ the sweep
FAILED=0

# Warm the page cache once before anything is timed. Without this the earliest
# runs read the topic off disk while later ones are served from RAM, which
# favours whichever configuration happens to run last. In the first
# three-machine run all the single-threaded measurements went first, from a
# cold cache, and were penalised for it.
msg "warming the page cache for topic '$TOPIC' (reading it once, untimed)"
warm_cache "$DATA" "$TOPIC"

if [ "$EXPERIMENT" = enhanced ]; then
    echo
    msg "Enhanced case: K parallel connections over the network (p=$PROB)"
    for k in $KLIST; do
        for r in $(seq 1 "$REPEATS"); do
            measure_dist enhanced 1 "$r" "$k" || FAILED=$((FAILED+1))
        done
    done
else
    # The two modes are interleaved inside each repeat rather than run as two
    # separate blocks, so wifi conditions drifting over the session affect both
    # equally instead of favouring whichever ran first.
    for n in $COUNTS; do
        echo
        msg "clients: $n  (modes interleaved)"
        for r in $(seq 1 "$REPEATS"); do
            for mode in $MODES; do
                measure_dist "$mode" "$n" "$r" || FAILED=$((FAILED+1))
            done
        done
    done
fi

echo
if [ "$FAILED" -eq 0 ]; then
    msg "every run completed with status=ok and crc_fail=0"
else
    msg "$FAILED run(s) did not reach status=ok - see $SUMMARY"
fi
msg "client metrics : $SUMMARY"
msg "server records : $SRVCSV   <- the 'peer' column proves three machines"
msg "server log     : $SRVLOG"
echo
msg "distinct client addresses the server saw:"
# Match addresses by shape rather than by column: the peer field sits in a
# different position in the Part I and Part II server CSVs, and reading a
# fixed column printed the topic name as if it were a client address.
grep -oE '\b[0-9]{1,3}(\.[0-9]{1,3}){3}\b' "$SRVCSV" 2>/dev/null | sort -u | sed 's/^/    /'
exit $([ "$FAILED" -eq 0 ] && echo 0 || echo 1)
