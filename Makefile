# resumable-file-transfer - top-level build and test entry point.
#
# A fresh clone needs nothing but a C compiler and Python 3:
#
#     make            build all four implementations
#     make test       unit tests, then the four functional suites
#     make check      everything CI runs: build, test, sanitizers, fuzz
#
# Every target generates the dataset it needs, so no manual setup step exists.

IMPLS     := baseline restart resume parallel
DATASET   := .dataset
WORK      := .work
PY        ?= python3

# Colours, but only when stdout is a terminal, so CI logs stay clean.
ifneq (,$(findstring xterm,$(TERM)))
  C_HDR := \033[1;36m
  C_OK  := \033[1;32m
  C_OFF := \033[0m
else
  C_HDR :=
  C_OK  :=
  C_OFF :=
endif

define hdr
	@printf '\n$(C_HDR)==> %s$(C_OFF)\n' "$(1)"
endef

# Targets here are ordered steps, not independent work: `test` runs the unit
# suite and then the functional suites, which bind TCP ports and share the
# generated corpus. Under `make -j` they would overlap and collide, so this
# makefile stays serial. Recursive sub-makes still parallelise, which is where
# the compile time actually is.
.NOTPARALLEL:

.PHONY: all build test check clean distclean dataset demo-dataset \
        unit functional sanitizers fuzz bench docker help $(IMPLS)

all: build

help:
	@printf 'targets:\n'
	@printf '  make build        compile all four implementations\n'
	@printf '  make test         unit tests + four functional suites\n'
	@printf '  make check        test + sanitizers + fuzz  (what CI runs)\n'
	@printf '  make bench        run the benchmark sweep, write Results/\n'
	@printf '  make fuzz         run the protocol fuzzer (FUZZ_SECONDS=30)\n'
	@printf '  make dataset      generate the test corpus into %s/\n' '$(DATASET)'
	@printf '  make docker       build the container image\n'
	@printf '  make clean        remove binaries and scratch space\n'

# ----------------------------------------------------------------- build
build: $(IMPLS)

$(IMPLS):
	$(call hdr,building $@)
	@$(MAKE) --no-print-directory -C src/$@

# ----------------------------------------------------------------- dataset
# The stamp file keeps `make test` from regenerating ~15 MB on every run.
dataset: $(DATASET)/.stamp

$(DATASET)/.stamp: tools/make_dataset.py
	$(call hdr,generating test dataset)
	@$(PY) tools/make_dataset.py --out $(DATASET)
	@touch $@

demo-dataset:
	$(call hdr,generating demo corpus)
	@$(PY) tools/make_dataset.py --out $(DATASET) --demo

# ----------------------------------------------------------------- tests
test: unit functional

unit: dataset
	$(call hdr,unit tests)
	@$(MAKE) --no-print-directory -C tests test

functional: build dataset
	@rc=0; \
	for t in $(IMPLS); do \
	    printf '\n$(C_HDR)==> functional: %s$(C_OFF)\n' "$$t"; \
	    ./tests/test_$$t.sh || rc=1; \
	done; \
	[ $$rc -eq 0 ] && printf '\n$(C_OK)all functional suites passed$(C_OFF)\n' || exit 1

sanitizers: dataset
	$(call hdr,sanitizers: ASan + UBSan + TSan)
	@./tests/test_sanitizers.sh baseline
	@./tests/test_sanitizers.sh resume

# ----------------------------------------------------------------- fuzz
FUZZ_SECONDS ?= 30

fuzz: dataset
	$(call hdr,protocol fuzzer ($(FUZZ_SECONDS)s))
	@$(MAKE) --no-print-directory -C fuzz run FUZZ_SECONDS=$(FUZZ_SECONDS)

# ----------------------------------------------------------------- check
check: test sanitizers fuzz
	@printf '\n$(C_OK)===== all checks passed =====$(C_OFF)\n'

# ----------------------------------------------------------------- bench
bench: build demo-dataset
	$(call hdr,benchmark sweep)
	@./bench/run_bench.sh

# ----------------------------------------------------------------- docker
docker:
	$(call hdr,building container image)
	@docker build -t resumable-file-transfer:latest .

# ----------------------------------------------------------------- clean
clean:
	@for d in $(IMPLS); do $(MAKE) --no-print-directory -C src/$$d clean; done
	@$(MAKE) --no-print-directory -C tests clean 2>/dev/null || true
	@$(MAKE) --no-print-directory -C fuzz clean 2>/dev/null || true
	@rm -rf $(WORK)
	@echo "cleaned"

distclean: clean
	@rm -rf $(DATASET)
	@echo "removed generated dataset"
