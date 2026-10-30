#!/usr/bin/env python3
"""Generate the datasets the tests, benchmarks and demo need.

File contents come from a seeded PRNG keyed on the file's own path, so every
run produces byte-identical output. That matters for two reasons: CRC32 values
are stable across machines, and a `diff -r` between the server's dataset and
the client's output directory is a meaningful correctness check rather than a
coincidence.

Three roots are produced:

  synthetic/       sizes chosen to land on and around every boundary in the
                   implementation - the 16 KiB chunk, the 64 KiB checkpoint
                   interval, the 512 KiB range-split threshold - plus a
                   zero-length file, which is the case that breaks naive
                   resume logic.
  synthetic_edge/  a topic with no files, and a topic with exactly one.
  demo/            larger topics shaped like the real corpus, for the
                   benchmarks and the Docker demo. Only with --demo.

Usage:
    tools/make_dataset.py                  # synthetic + edge, into .dataset/
    tools/make_dataset.py --demo           # also the larger demo corpus
    tools/make_dataset.py --out /tmp/data  # somewhere else
"""

from __future__ import annotations

import argparse
import os
import random
import sys

CHUNK = 16 * 1024          # protocol chunk size
CKPT = 64 * 1024           # client checkpoint interval
SPLIT = 512 * 1024         # range-split threshold in the parallel client


def write_file(path: str, size: int) -> int:
    """Write `size` deterministic bytes to `path`, keyed on the path itself."""
    rnd = random.Random(path.replace(os.sep, "/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        left = size
        while left > 0:
            n = min(left, 1 << 20)
            fh.write(rnd.randbytes(n))
            left -= n
    return size


def build(root: str, plan: dict[str, list[tuple[str, int]]], label: str) -> tuple[int, int]:
    total = count = 0
    for topic in sorted(plan):
        files = plan[topic]
        for name, size in files:
            write_file(os.path.join(root, topic, name), size)
        n = sum(s for _, s in files)
        print("  %-14s %5d files %12s" % (topic, len(files), human(n)))
        total += n
        count += len(files)
    print("  %-14s %5d files %12s   %s" % ("TOTAL", count, human(total), label))
    return count, total


def human(n: int) -> str:
    for unit in ("B", "KiB", "MiB", "GiB"):
        if n < 1024 or unit == "GiB":
            return "%.1f %s" % (n, unit) if unit != "B" else "%d B" % n
        n /= 1024.0
    return str(n)


def plan_synthetic() -> dict[str, list[tuple[str, int]]]:
    """Boundary-focused. Every size here exists to break something specific."""
    boundary = [
        0,                 # zero-length: the case that makes (file_idx, 0) vs
                           # (file_idx-1, size) ambiguous if you get it wrong
        1, 100, 1023, 1024, 4095, 4096,
        CHUNK - 1, CHUNK, CHUNK + 1,          # exactly one protocol chunk
        2 * CHUNK,
        CKPT - 1, CKPT, CKPT + 1,             # exactly one checkpoint interval
        5 * 1024 * 1024,                      # several hundred chunks
    ]
    alpha = [("a%02d_%d.bin" % (i, s), s) for i, s in enumerate(boundary)]

    # Shaped like the real image corpus: many small files, mean around 23 KiB.
    rnd = random.Random(1234)
    beta = [("b%03d.bin" % i, rnd.randint(1455, 60000)) for i in range(200)]

    # Straddling the range-split threshold, so the parallel client exercises
    # both the "send whole file" and "cut into K ranges" paths.
    gamma_sizes = [SPLIT - 1, SPLIT, SPLIT + 1, 2 * SPLIT, 4 * SPLIT + 777]
    gamma = [("g%d_%d.bin" % (i, s), s) for i, s in enumerate(gamma_sizes)]

    return {"topic_alpha": alpha, "topic_beta": beta, "topic_gamma": gamma}


def plan_demo() -> dict[str, list[tuple[str, int]]]:
    """Bigger, realistic topics. Sized so a run takes seconds, not minutes."""
    rnd = random.Random(99)
    return {
        # many small files - the shape that stresses per-file overhead
        "images": [("img_%04d.bin" % i, rnd.randint(8_000, 40_000)) for i in range(1200)],
        # mid-sized, the general case
        "documents": [("doc_%03d.bin" % i, rnd.randint(60_000, 400_000)) for i in range(220)],
        # large files, so the parallel client actually splits ranges
        "archives": [("arc_%02d.bin" % i, rnd.randint(2_000_000, 6_000_000)) for i in range(12)],
    }


def main() -> int:
    here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default=os.path.join(here, ".dataset"),
                    help="target directory (default: <repo>/.dataset)")
    ap.add_argument("--demo", action="store_true",
                    help="also generate the larger demo corpus")
    ap.add_argument("--force", action="store_true",
                    help="regenerate even if the target already exists")
    args = ap.parse_args()

    synth = os.path.join(args.out, "synthetic")
    edge = os.path.join(args.out, "synthetic_edge")
    demo = os.path.join(args.out, "demo")

    if os.path.isdir(synth) and not args.force:
        print("dataset already present at %s (use --force to regenerate)" % args.out)
        if not (args.demo and not os.path.isdir(demo)):
            return 0

    if not os.path.isdir(synth) or args.force:
        print("synthetic/ - boundary cases")
        build(synth, plan_synthetic(), "")

        print("synthetic_edge/ - degenerate topics")
        os.makedirs(os.path.join(edge, "topic_empty"), exist_ok=True)
        write_file(os.path.join(edge, "topic_one", "only.bin"), 7)
        print("  %-14s %5d files %12s" % ("topic_empty", 0, "0 B"))
        print("  %-14s %5d files %12s" % ("topic_one", 1, "7 B"))

    if args.demo and (not os.path.isdir(demo) or args.force):
        print("demo/ - benchmark and demo corpus")
        build(demo, plan_demo(), "")

    print("\ndataset root: %s" % args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
