# Part2 Case 1 - fault injection, no session management.
#
# `make` produces exactly two executables in this folder: server and client.
# The server is always multi-threaded, one thread per client, as Part II requires.
#
#   ./server <port> <topics_root_dir> <failure_probability>
#   ./client <server_host> <server_port> <topic> <output_dir>
#
# The client retries automatically until the topic is complete; it needs no
# extra arguments and no manual steps.
#
# The shared library in ../common is compiled by relative path, so this folder
# builds as-is inside the submission tree.  COMMON_DIR can be overridden if the
# sources are ever vendored into this directory instead.

CC         := gcc
COMMON_DIR ?= ../common
CSTD       := -std=c11
WARN       := -Wall -Wextra -Werror
OPT        := -O2 -g
CFLAGS     := $(CSTD) $(WARN) $(OPT) -D_GNU_SOURCE
LDLIBS     := -lpthread

COMMON_SRC := $(COMMON_DIR)/common.c $(COMMON_DIR)/crc32.c $(COMMON_DIR)/logging.c $(COMMON_DIR)/netutil.c $(COMMON_DIR)/protocol.c $(COMMON_DIR)/topic_table.c $(COMMON_DIR)/fault_inject.c $(COMMON_DIR)/cliargs.c
COMMON_HDR := $(wildcard $(COMMON_DIR)/*.h)

.PHONY: all clean
all: server client

server: server.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ server.c $(COMMON_SRC) $(LDLIBS)

client: client.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ client.c $(COMMON_SRC) $(LDLIBS)

clean:
	rm -rf server client build

# ---------------------------------------------------------------- analysis
# Instrumented builds, used by tests/test_sanitizers.sh.  They go into build/
# so that `make` still leaves exactly two executables in this directory.
#
#   make asan    AddressSanitizer + UndefinedBehaviorSanitizer
#   make tsan    ThreadSanitizer
#   make debug   -O0 -g, for valgrind and gdb
SANFLAGS := -fsanitize=address,undefined -fno-omit-frame-pointer
TSNFLAGS := -fsanitize=thread

.PHONY: asan tsan debug
asan:
	mkdir -p build
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE $(SANFLAGS) -o build/server_asan server.c $(COMMON_SRC) $(LDLIBS)
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE $(SANFLAGS) -o build/client_asan client.c $(COMMON_SRC) $(LDLIBS)

tsan:
	mkdir -p build
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE $(TSNFLAGS) -o build/server_tsan server.c $(COMMON_SRC) $(LDLIBS)
	$(CC) $(CSTD) $(WARN) -O1 -g -D_GNU_SOURCE $(TSNFLAGS) -o build/client_tsan client.c $(COMMON_SRC) $(LDLIBS)

debug:
	mkdir -p build
	$(CC) $(CSTD) $(WARN) -O0 -g -D_GNU_SOURCE -o build/server_dbg server.c $(COMMON_SRC) $(LDLIBS)
	$(CC) $(CSTD) $(WARN) -O0 -g -D_GNU_SOURCE -o build/client_dbg client.c $(COMMON_SRC) $(LDLIBS)
