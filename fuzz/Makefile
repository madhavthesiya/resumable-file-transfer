# Protocol fuzzer.
#
#   make run                 standalone, FUZZ_SECONDS long (CI uses this)
#   make libfuzzer           coverage-guided build, needs clang
#   make run-libfuzzer       build it and run for FUZZ_SECONDS
#
# The standalone build is the default because it needs nothing but the C
# compiler already required to build the project, so CI gets fuzzing on every
# push without a toolchain install.

CC           ?= gcc
CLANG        ?= clang
CSTD         := -std=c11
WARN         := -Wall -Wextra
OPT          := -O1 -g
CFLAGS       := $(CSTD) $(WARN) $(OPT) -D_GNU_SOURCE
LDLIBS       := -lpthread
FUZZ_SECONDS ?= 30
CORPUS       := corpus

COMMON_DIR := ../src/common
COMMON_SRC := $(COMMON_DIR)/common.c   $(COMMON_DIR)/crc32.c      \
              $(COMMON_DIR)/logging.c  $(COMMON_DIR)/netutil.c    \
              $(COMMON_DIR)/protocol.c $(COMMON_DIR)/topic_table.c \
              $(COMMON_DIR)/fault_inject.c $(COMMON_DIR)/cliargs.c

.PHONY: all run libfuzzer run-libfuzzer clean

all: fuzz_protocol

# Standalone. ASan is on because a fuzzer without a sanitiser only catches the
# crashes that happen to be fatal on their own.
fuzz_protocol: fuzz_protocol.c $(COMMON_SRC)
	$(CC) $(CFLAGS) -fsanitize=address,undefined -fno-omit-frame-pointer \
	    -o $@ $^ $(LDLIBS)

run: fuzz_protocol
	ASAN_OPTIONS=detect_leaks=1 ./fuzz_protocol $(FUZZ_SECONDS)

libfuzzer: fuzz_protocol_lf

fuzz_protocol_lf: fuzz_protocol.c $(COMMON_SRC)
	@command -v $(CLANG) >/dev/null 2>&1 || \
	    { echo "clang not found; the standalone build (make run) needs no clang"; exit 1; }
	$(CLANG) $(CSTD) $(WARN) $(OPT) -D_GNU_SOURCE -DFUZZ_LIBFUZZER \
	    -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer \
	    -o $@ $^ $(LDLIBS)

run-libfuzzer: fuzz_protocol_lf
	mkdir -p $(CORPUS)
	./fuzz_protocol_lf $(CORPUS) -max_total_time=$(FUZZ_SECONDS) -print_final_stats=1

clean:
	rm -rf fuzz_protocol fuzz_protocol_lf $(CORPUS) crash-* leak-* timeout-*
