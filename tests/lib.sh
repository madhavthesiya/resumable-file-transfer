#!/bin/bash
# Shared helpers for the test scripts.  Sourced, not executed.

PASS=0
FAIL=0

ok()   { PASS=$((PASS+1)); printf '  \033[32mPASS\033[0m %s\n' "$*"; }
bad()  { FAIL=$((FAIL+1)); printf '  \033[31mFAIL\033[0m %s\n' "$*"; }
note() { printf '  ---- %s\n' "$*"; }
hdr()  { printf '\n== %s ==\n' "$*"; }

# check <description> <expected> <actual>
check_eq() {
    if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$2', got '$3')"; fi
}

# Pick a TCP port that nothing is listening on, starting from $1.
free_port() {
    local p=${1:-29000}
    while ss -ltn 2>/dev/null | grep -q ":$p "; do p=$((p+1)); done
    echo "$p"
}

# Block until something is listening on $1, or fail after ~5 s.
wait_port() {
    local p=$1 i=0
    while [ $i -lt 100 ]; do
        ss -ltn 2>/dev/null | grep -q ":$p " && return 0
        sleep 0.05; i=$((i+1))
    done
    return 1
}

# Compare two directory trees byte for byte, both by content and by file set.
# diff -r catches content and missing/extra files; the sha256 pass is a second,
# independent check that does not rely on diff's own traversal.
compare_trees() {
    local want=$1 got=$2 label=$3
    if diff -r "$want" "$got" > /dev/null 2>&1; then
        ok "$label: diff -r is clean"
    else
        bad "$label: diff -r reported differences"
        diff -r "$want" "$got" 2>&1 | head -10
        return 1
    fi
    local a b
    a=$(cd "$want" && find . -type f -printf '%P\n' | LC_ALL=C sort | xargs -r sha256sum | sha256sum | cut -d' ' -f1)
    b=$(cd "$got"  && find . -type f -printf '%P\n' | LC_ALL=C sort | xargs -r sha256sum | sha256sum | cut -d' ' -f1)
    check_eq "$label: aggregate sha256 of all files" "$a" "$b"
}

# Extract one field from a client RESULT line: result_field <file> <key>
result_field() { sed -n 's/.*[[:space:]]'"$2"'=\([^[:space:]]*\).*/\1/p' "$1" | head -1; }

summary() {
    printf '\n%s: %d passed, %d failed\n' "${1:-SUMMARY}" "$PASS" "$FAIL"
    [ "$FAIL" -eq 0 ]
}
