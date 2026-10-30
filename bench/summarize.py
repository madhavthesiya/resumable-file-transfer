#!/usr/bin/env python3
"""Turn bench_runs.csv into the summary tables in Results/SUMMARY.md.

    python3 summarize.py [runs.csv] [out.md]

Rows that share everything except `repeat` are one data point; the repeats are
reduced to mean and spread.  Nothing here re-runs or re-measures: the CSV is the
single source of truth, so the tables can always be regenerated from it.
"""
import csv
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "docs", "results", "bench_runs.csv")
DST = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "..", "docs", "results", "SUMMARY.md")

MB = 1024.0 * 1024.0


# The implementations were renamed from their original assignment labels when
# this became a public repository. Result files recorded before that rename are
# kept verbatim as historical data, so both spellings are normalised on read
# and the archived runs summarise exactly as they always did.
CASE_ALIASES = {
    "part1": "baseline",
    "part2_case1": "restart",
    "part2_case2": "resume",
    "part2_case2_enhanced": "parallel",
}


def load(path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    for r in rows:
        if r.get("case") in CASE_ALIASES:
            r["case"] = CASE_ALIASES[r["case"]]
        for k in ("useful_bytes", "redundant_bytes", "wire_rx_bytes", "files",
                  "reconnects", "retries", "crc_ok", "crc_fail", "clients"):
            r[k] = int(r[k] or 0)
        for k in ("makespan_s", "agg_goodput_mbps", "overhead_pct",
                  "wall_span_s", "max_client_elapsed_s"):
            r[k] = float(r.get(k) or 0.0)
    return rows


def group(rows, keys):
    """Collapse repeats: returns {key_tuple: [rows]} preserving first-seen order."""
    out = {}
    for r in rows:
        out.setdefault(tuple(r[k] for k in keys), []).append(r)
    return out


def ms(vals, fmt="%.2f"):
    """mean and spread, formatted; spread is omitted for a single sample.

    These runs are noisy by nature - the fault draw is per chunk, and a
    loopback transfer competes with the page cache and the scheduler - so a
    mean without its spread would overstate how settled these numbers are.
    """
    if not vals:
        return "-"
    m = statistics.mean(vals)
    if len(vals) < 2:
        return fmt % m
    return ("%s &plusmn;%s" % (fmt, fmt)) % (m, statistics.pstdev(vals))


def mean(vals):
    return statistics.mean(vals) if vals else 0.0


def table(fh, header, aligns, rows):
    fh.write("| " + " | ".join(header) + " |\n")
    fh.write("|" + "|".join(aligns) + "|\n")
    for r in rows:
        fh.write("| " + " | ".join(str(c) for c in r) + " |\n")
    fh.write("\n")


def main():
    rows = load(SRC)
    if not rows:
        sys.exit("no rows in %s" % SRC)

    with open(DST, "w", newline="\n") as fh:
        fh.write("# Results\n\n")
        fh.write("Generated from `bench_runs.csv` by `bench/summarize.py`; "
                 "every number below is the mean over the repeats recorded there.\n\n")

        # ---------------------------------------------------------- integrity
        bad = [r for r in rows if r["status"] != "ok" or r["crc_fail"]]
        fh.write("## Integrity\n\n")
        fh.write("- runs recorded: **%d**\n" % len(rows))
        fh.write("- runs reaching `status=ok`: **%d**\n"
                 % sum(1 for r in rows if r["status"] == "ok"))
        fh.write("- files verified by CRC32: **%d**\n" % sum(r["crc_ok"] for r in rows))
        fh.write("- CRC mismatches: **%d**\n" % sum(r["crc_fail"] for r in rows))
        fh.write("- bytes delivered: **%.1f GB**\n"
                 % (sum(r["useful_bytes"] for r in rows) / MB / 1024))
        if bad:
            fh.write("\n**%d run(s) did not verify:**\n\n" % len(bad))
            for r in bad:
                fh.write("- `%s` %s %s p=%s K=%s N=%s r=%s -> status=%s crc_fail=%d\n"
                         % (r["exp"], r["case"], r["topic"], r["p"], r["K"],
                            r["clients"], r["repeat"], r["status"], r["crc_fail"]))
        else:
            fh.write("\nEvery recorded run completed and every received file matched "
                     "its CRC32.\n")
        fh.write("\n> Throughput figures are **loopback** measurements on one host: "
                 "they are bounded by memory and scheduler behaviour, not by a "
                 "network link, so compare them against each other and not against "
                 "a link rate.\n\n")

        # ---------------------------------------------------- E1 concurrency
        e1 = [r for r in rows if r["exp"] == "e1"]
        if e1:
            fh.write("## E1 - baseline: threading mode vs concurrent clients\n\n")
            fh.write("Topic `%s`, no faults. Makespan is wall-clock until the last "
                     "client finishes; goodput is total useful bytes over that "
                     "makespan.\n\n" % e1[0]["topic"])
            g = group(e1, ["mode", "clients"])
            # `clients` is an int in the parsed rows, so the lookup key must be
            # an int too - comparing against str(n) here silently missed every
            # group and printed a table of dashes.
            present = {k[0] for k in g}
            modes = [m for m in ("single", "multi") if m in present]
            modes += sorted(present - set(modes))
            ns = sorted({k[1] for k in g})
            table(fh,
                  ["clients"] + ["%s: makespan s" % m for m in modes]
                              + ["%s: Mbit/s" % m for m in modes],
                  ["---:"] * (1 + 2 * len(modes)),
                  [[n] + [ms([r["makespan_s"] for r in g.get((m, n), [])])
                          for m in modes]
                        + [ms([r["agg_goodput_mbps"] for r in g.get((m, n), [])], "%.0f")
                           for m in modes]
                   for n in ns])

        # ------------------------------------------------------- E2 size scan
        e2 = [r for r in rows if r["exp"] == "e2"]
        if e2:
            fh.write("## E2 - baseline: goodput vs topic size\n\n")
            g = group(e2, ["topic"])
            table(fh, ["topic", "files", "MB", "makespan s", "Mbit/s"],
                  ["---", "---:", "---:", "---:", "---:"],
                  [[k[0], rs[0]["files"], "%.1f" % (rs[0]["useful_bytes"] / MB),
                    ms([r["makespan_s"] for r in rs]),
                    ms([r["agg_goodput_mbps"] for r in rs], "%.0f")]
                   for k, rs in sorted(g.items(),
                                       key=lambda kv: kv[1][0]["useful_bytes"])])

        # ------------------------------------- E3/E4 Case 1 vs Case 2 by fault rate
        for exp, title, note in (
            ("e3", "E3 - restart vs resume across the fault rate",
             "restart has no checkpoint, so a fault restarts the whole topic; "
             "resume continues from the last checkpoint. `redundant` is bytes that "
             "had to be sent more than once."),
            ("e4", "E4 - restart vs resume on a large-file topic",
             "The same comparison where single files are large, which is where "
             "restart-from-scratch costs the most."),
        ):
            er = [r for r in rows if r["exp"] == exp]
            if not er:
                continue
            fh.write("## %s\n\n" % title)
            fh.write("%s\n\n" % note)
            g = group(er, ["p", "case"])
            cases = sorted({k[1] for k in g})
            ps = sorted({k[0] for k in g}, key=float)
            for case in cases:
                fh.write("**%s** (topic `%s`)\n\n" % (case, er[0]["topic"]))
                table(fh, ["p", "expected faults/pass", "makespan s", "Mbit/s",
                           "redundant MB", "x topic size", "reconnects"],
                      ["---:"] * 7,
                      [[p,
                        "%.2f" % (float(p) * _chunks(er[0]["topic"])),
                        ms([r["makespan_s"] for r in g[(p, case)]]),
                        ms([r["agg_goodput_mbps"] for r in g[(p, case)]], "%.0f"),
                        ms([r["redundant_bytes"] / MB for r in g[(p, case)]], "%.1f"),
                        "%.2f" % (mean([r["redundant_bytes"] for r in g[(p, case)]])
                                  / max(1, mean([r["useful_bytes"] for r in g[(p, case)]]))),
                        "%.1f" % mean([r["reconnects"] for r in g[(p, case)]])]
                       for p in ps if (p, case) in g])

        # ----------------------------------------------------- E5/E6 Enhanced
        for exp, title, note in (
            ("e5", "E5 - parallel: K parallel connections, fault-free",
             "One client, K TCP connections to the same server thread, which "
             "multiplexes them with epoll."),
            ("e6", "E6 - parallel under faults",
             "Same topic and p, comparing a single connection against K=4."),
        ):
            er = [r for r in rows if r["exp"] == exp]
            if not er:
                continue
            fh.write("## %s\n\n" % title)
            fh.write("%s\n\n" % note)
            g = group(er, ["K"])
            base = None
            out = []
            # group() keys are tuples, so sort on the single element they hold.
            for key in sorted(g, key=lambda t: int(t[0])):
                k = key[0]
                rs = g[key]
                gp = mean([r["agg_goodput_mbps"] for r in rs])
                if base is None:
                    base = gp
                out.append([k, ms([r["makespan_s"] for r in rs]),
                            ms([r["agg_goodput_mbps"] for r in rs], "%.0f"),
                            "%.2fx" % (gp / base if base else 0),
                            "%.1f" % mean([r["reconnects"] for r in rs]),
                            "%.1f" % (mean([r["redundant_bytes"] for r in rs]) / MB)])
            table(fh, ["K", "makespan s", "Mbit/s", "speedup", "reconnects",
                       "redundant MB"], ["---:"] * 6, out)

        # ---------------------------------- E7 fault-free, all on one topic
        e7 = [r for r in rows if r["exp"] == "e7"]
        if e7:
            fh.write("## E7 - fault-free comparison on a single topic\n\n")
            fh.write("Every implementation on topic `%s`, one client, p=0. The other "
                     "experiments each use whichever topic suits what they measure, so "
                     "their p=0 rows sit on different workloads and cannot be divided "
                     "by one another; this experiment exists to make the comparison "
                     "like-for-like.\n\n" % e7[0]["topic"])
            groups = [("baseline", None, "baseline (multi-threaded)"),
                      ("restart",  None, "restart (no session)"),
                      ("resume",   None, "resume (session management)"),
                      ("parallel", "1",  "parallel, K=1"),
                      ("parallel", "4",  "parallel, K=4")]
            out, ref = [], None
            for case, k, lab in groups:
                rs = [r for r in e7 if r["case"] == case
                      and (k is None or str(r["K"]) == k)]
                if not rs:
                    continue
                gp = mean([r["agg_goodput_mbps"] for r in rs])
                if ref is None:
                    ref = gp
                out.append([lab, ms([r["makespan_s"] for r in rs]),
                            ms([r["agg_goodput_mbps"] for r in rs], "%.0f"),
                            "%.2fx" % (gp / ref if ref else 0),
                            "%.3f" % mean([r["overhead_pct"] for r in rs])])
            table(fh, ["implementation", "makespan s", "Mbit/s", "vs baseline",
                       "overhead %"],
                  ["---", "---:", "---:", "---:", "---:"], out)

    print("wrote %s" % os.path.normpath(DST))


_CHUNKS = {"scoiattolo": 1551, "mucca": 1577, "cavallo": 2284, "gallina": 3165,
           "elefante": 3572, "cane": 3705, "farfalla": 3777, "pecora": 3823,
           "ragno": 4822, "gatto": 9226}


def _chunks(topic):
    return _CHUNKS.get(topic, 0)


if __name__ == "__main__":
    main()
