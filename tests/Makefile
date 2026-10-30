# Unit tests for the common/ library.  Not part of the graded deliverable, but
# `make -C tests test` is the first gate every other stage depends on.
#
# Targets:
#   make test   build and run with the default flags
#   make asan   build and run under AddressSanitizer + UndefinedBehaviorSanitizer
#   make tsan   build and run under ThreadSanitizer
#   make clean

CC      := gcc
CSTD    := -std=c11
WARN    := -Wall -Wextra -Werror
OPT     := -O2 -g
CFLAGS  := $(CSTD) $(WARN) $(OPT) -D_GNU_SOURCE
LDLIBS  := -lpthread

COMMON_DIR := ../src/common
COMMON_SRC := $(COMMON_DIR)/common.c   $(COMMON_DIR)/crc32.c    \
              $(COMMON_DIR)/logging.c  $(COMMON_DIR)/netutil.c   \
              $(COMMON_DIR)/protocol.c $(COMMON_DIR)/topic_table.c \
              $(COMMON_DIR)/fault_inject.c $(COMMON_DIR)/cliargs.c

SYNTH ?= ../.dataset/synthetic
TMPD  ?= ../.work/unit

.PHONY: test asan tsan clean
test: test_common
	./test_common $(SYNTH) $(TMPD)

test_common: test_common.c $(COMMON_SRC)
	$(CC) $(CFLAGS) -o $@ $^ $(LDLIBS)

asan: test_common.c $(COMMON_SRC)
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE -fsanitize=address,undefined \
	    -fno-omit-frame-pointer -o test_common_asan $^ $(LDLIBS)
	ASAN_OPTIONS=detect_leaks=1 ./test_common_asan $(SYNTH) $(TMPD)

# ThreadSanitizer needs a predictable address space.  WSL2 kernels randomise
# mmap with more bits than TSan supports, which aborts the run with
# "unexpected memory mapping"; setarch -R disables ASLR for the child only,
# and is skipped where setarch does not exist.
tsan: test_common.c $(COMMON_SRC)
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE -fsanitize=thread -o test_common_tsan $^ $(LDLIBS)
	@sh -c 'if command -v setarch >/dev/null 2>&1; then setarch $$(uname -m) -R ./test_common_tsan $(SYNTH) $(TMPD); else ./test_common_tsan $(SYNTH) $(TMPD); fi'

clean:
	rm -f test_common test_common_asan test_common_tsan
