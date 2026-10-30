#!/bin/bash
# Shared helpers for the benchmark harness.  Sourced, never executed.

# ---------------------------------------------------------------- utilities
msg()  { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }

free_port() {
    local p=${1:-33000}
    while ss -ltn 2>/dev/null | grep -q ":$p "; do p=$((p+1)); done
    echo "$p"
}

wait_port() {
    local p=$1 limit=${2:-600} i=0
    while [ $i -lt "$limit" ]; do
        ss -ltn 2>/dev/null | grep -q ":$p " && return 0
        sleep 0.1; i=$((i+1))
    done
    return 1
}

# Read one field out of a client RESULT line.
rf() { sed -n 's/.*[[:space:]]'"$2"'=\([^[:space:]]*\).*/\1/p' "$1" 2>/dev/null | head -1; }

# Sum a field across several RESULT files.  Missing values count as 0.
sum_field() {
    local key=$1; shift
    local total=0 v
    for f in "$@"; do
        v=$(rf "$f" "$key")
        case "$v" in ''|*[!0-9]*) v=0 ;; esac
        total=$((total + v))
    done
    echo "$total"
}

# Maximum of a floating-point field across several RESULT files.
max_field_f() {
    local key=$1; shift
    local best=0 v
    for f in "$@"; do
        v=$(rf "$f" "$key")
        [ -z "$v" ] && continue
        best=$(awk -v a="$best" -v b="$v" 'BEGIN{print (b>a)?b:a}')
    done
    echo "$best"
}

# Page-cache warm-up.  Every measurement here is meant to compare network and
# protocol behaviour, so the first-touch cost of reading 600 MB off a virtual
# disk must not land inside one of the timed runs.  Skipped with --no-warm.
warm_cache() {
    local root=$1 topic=$2
    [ "${NO_WARM:-0}" = "1" ] && return 0
    if [ -n "$topic" ] && [ -d "$root/$topic" ]; then
        find "$root/$topic" -type f -print0 2>/dev/null | xargs -0 -r cat > /dev/null 2>&1
    else
        find "$root" -type f -print0 2>/dev/null | xargs -0 -r cat > /dev/null 2>&1
    fi
}

# ------------------------------------------------------- launching a client
# launch_client <slot> <host> <bindir> <server_ip> <port> <topic> <outroot> <resultfile> [extra args...]
#
# `host` is either "localhost" (run here) or an ssh destination.  Everything
# else is identical, which is what lets one harness drive both the localhost
# development runs and the three-machine runs without changing the metrics.
#
# The pid is returned in the global LAST_CLIENT_PID rather than on stdout,
# because a caller using `pid=$(launch_client ...)` would run this function in
# a subshell: the background client would then be a child of that subshell and
# the caller's `wait "$pid"` would fail with "not a child of this shell".
LAST_CLIENT_PID=""
launch_client() {
    local slot=$1 host=$2 bindir=$3 sip=$4 port=$5 topic=$6 outroot=$7 res=$8
    shift 8
    local out="$outroot/c$slot"
    if [ "$host" = "localhost" ]; then
        rm -rf -- "${out:?launch_client: output path is empty}"
        "$bindir/client" "$sip" "$port" "$topic" "$out" "$@" > "$res" 2>"${res%.out}.err" &
        LAST_CLIENT_PID=$!
    else
        # -n stops ssh consuming the harness's stdin; -T avoids a pty, which
        # would otherwise mangle the RESULT line with carriage returns.
        #
        # SSH_OPTS_STR lets the caller pass ControlMaster settings so every
        # client reuses one already-established connection per host. Without
        # it each launch pays a fresh handshake, and that 100-300 ms would be
        # charged to the measured makespan.
        local opts="${SSH_OPTS_STR:--o BatchMode=yes -o StrictHostKeyChecking=accept-new}"
        # shellcheck disable=SC2086
        ssh -n -T $opts "$host" \
            "rm -rf '$out' && '$bindir/client' '$sip' '$port' '$topic' '$out' $*" \
            > "$res" 2>"${res%.out}.err" &
        LAST_CLIENT_PID=$!
    fi
}

# Remote preparation: copy the submission tree to each client host and build it.
prepare_client_hosts() {
    local srcdir=$1 destdir=$2; shift 2
    local host rc=0
    for host in "$@"; do
        [ "$host" = "localhost" ] && continue
        msg "preparing $host:$destdir"
        ssh -n -o BatchMode=yes -o StrictHostKeyChecking=accept-new "$host" \
            "mkdir -p '$destdir'" || { msg "cannot reach $host"; rc=1; continue; }
        rsync -az --delete \
            --exclude 'build/' --exclude 'Results/' --exclude '*.o' \
            "$srcdir/" "$host:$destdir/" || { msg "rsync to $host failed"; rc=1; continue; }
        ssh -n -o BatchMode=yes "$host" \
            "cd '$destdir' && for d in baseline restart resume parallel; do make -C \$d clean >/dev/null && make -C \$d >/dev/null || exit 1; done" \
            || { msg "remote build on $host failed"; rc=1; continue; }
        msg "$host ready"
    done
    return $rc
}
