#!/bin/bash
# One entrypoint, both roles, so the compose file can run the same image three
# times. Everything is driven by environment variables with working defaults,
# because a demo that needs arguments is a demo nobody runs.
#
#   server  [IMPL]              start a server    (IMPL: baseline|restart|resume|parallel)
#   client  [IMPL] [TOPIC]      fetch a topic
#   demo                        run the whole comparison, printing a summary
#   shell                       drop into bash
set -u

IMPL=${IMPL:-baseline}
TOPIC=${TOPIC:-images}
DATASET=${DATASET:-/dataset/demo}
PORT=${PORT:-34000}
SERVER=${SERVER:-server}
MODE=${MODE:-multi}            # baseline only: single | multi
PROB=${PROB:-0}                # fault probability, the fault-tolerant impls
CONNS=${CONNS:-4}              # parallel only
CLIENTS=${CLIENTS:-2}          # clients started per container
OUT=${OUT:-/tmp/out}

say() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

wait_for_server() {
    local host=$1 port=$2 i=0
    while [ $i -lt 120 ]; do
        if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
        sleep 0.5; i=$((i+1))
    done
    echo "timed out waiting for $host:$port" >&2
    return 1
}

run_server() {
    local impl=${1:-$IMPL}
    case "$impl" in
        baseline) say "baseline server on :$PORT ($MODE-threaded)"
                  exec baseline-server "$PORT" "$DATASET" "$MODE" ;;
        restart)  say "restart server on :$PORT (p=$PROB)"
                  exec restart-server  "$PORT" "$DATASET" "$PROB" ;;
        resume)   say "resume server on :$PORT (p=$PROB)"
                  exec resume-server   "$PORT" "$DATASET" "$PROB" ;;
        parallel) say "parallel server on :$PORT (p=$PROB)"
                  exec parallel-server "$PORT" "$DATASET" "$PROB" ;;
        *) echo "unknown implementation: $impl" >&2; exit 2 ;;
    esac
}

run_client() {
    local impl=${1:-$IMPL} topic=${2:-$TOPIC}
    wait_for_server "$SERVER" "$PORT" || exit 1

    local pids=()
    for i in $(seq 1 "$CLIENTS"); do
        local out="$OUT/${impl}_$i"
        rm -rf -- "${out:?}"
        case "$impl" in
            baseline) baseline-client "$SERVER" "$PORT" "$topic" "$out" & ;;
            restart)  restart-client  "$SERVER" "$PORT" "$topic" "$out" & ;;
            resume)   resume-client   "$SERVER" "$PORT" "$topic" "$out" & ;;
            parallel) parallel-client "$SERVER" "$PORT" "$topic" "$out" "$CONNS" & ;;
            *) echo "unknown implementation: $impl" >&2; exit 2 ;;
        esac
        pids+=($!)
    done

    local rc=0
    for p in "${pids[@]}"; do wait "$p" || rc=1; done
    exit $rc
}

case "${1:-server}" in
    server) shift; run_server "$@" ;;
    client) shift; run_client "$@" ;;
    shell)  exec /bin/bash ;;
    *)      exec "$@" ;;
esac
