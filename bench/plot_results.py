#!/usr/bin/env python3
"""Draw the result graphs from bench_runs.csv.

    python3 plot_results.py [runs.csv] [outdir]

Writes one SVG per graph into Results/.  `make_pdfs.sh` then renders each one
to PDF with headless Chromium, which is what the submission ships.

Deliberately dependency-free: the measurement machine has no matplotlib, no
numpy and no pip, so the SVG is emitted directly.  That also keeps the graphs
reproducible from the CSV alone, with nothing to install.
"""
import csv
import math
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "..", "docs", "results", "bench_runs.csv")
OUT = sys.argv[2] if len(sys.argv) > 2 else os.path.join(HERE, "..", "docs", "results")

MB = 1024.0 * 1024.0
W, H = 760, 440
PAD_L, PAD_R, PAD_T, PAD_B = 92, 28, 54, 76

# Colour-blind-safe, and legible when the PDF is printed in greyscale because
# each series also differs in dash pattern and marker.
PALETTE = ["#1f5fbf", "#d1600f", "#0f8a5f", "#7b3fbf", "#b01e53"]
DASHES = ["", "7,4", "2,3", "10,3,2,3", "5,2"]


def esc(s):
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# Result files recorded before the implementations were renamed are kept
# verbatim as historical data; both spellings are normalised on read so the
# archived runs plot exactly as they always did. Mirrors summarize.py.
CASE_ALIASES = {
    "part1": "baseline",
    "part2_case1": "restart",
    "part2_case2": "resume",
    "part2_case2_enhanced": "parallel",
}


def load(path):
    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    out = []
    for r in rows:
        if r.get("status") != "ok":
            continue
        if r.get("case") in CASE_ALIASES:
            r["case"] = CASE_ALIASES[r["case"]]
        for k in ("useful_bytes", "redundant_bytes", "wire_rx_bytes", "reconnects",
                  "clients", "files"):
            r[k] = int(r[k] or 0)
        for k in ("makespan_s", "agg_goodput_mbps", "overhead_pct"):
            r[k] = float(r[k] or 0.0)
        out.append(r)
    return out


def agg(rows, key):
    """mean and population stdev of `key` over rows."""
    vals = [r[key] if not callable(key) else key(r) for r in rows]
    if not vals:
        return 0.0, 0.0
    return statistics.mean(vals), (statistics.pstdev(vals) if len(vals) > 1 else 0.0)


def nice_ticks(lo, hi, n=5):
    """Round axis bounds outward to human-friendly tick positions."""
    if hi <= lo:
        hi = lo + 1.0
    raw = (hi - lo) / n
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    for m in (1, 2, 2.5, 5, 10):
        if raw / mag <= m:
            step = m * mag
            break
    else:
        step = 10 * mag
    start = math.floor(lo / step) * step
    end = math.ceil(hi / step) * step
    ticks, v = [], start
    while v <= end + step * 1e-9:
        ticks.append(round(v, 10))
        v += step
    return start, end, ticks


def fmt(v):
    if v == 0:
        return "0"
    if abs(v) >= 1000:
        return "%.0f" % v
    if abs(v) >= 10:
        return "%.0f" % v
    if abs(v) >= 1:
        return "%.1f" % v
    return ("%.3f" % v).rstrip("0").rstrip(".")


def frame(title, subtitle, xlabel, ylabel, body, xticks, yticks, x0, x1, y0, y1,
          legend):
    """Assemble one finished SVG from a pre-rendered plot body."""
    def sx(v):
        return PAD_L + (v - x0) / (x1 - x0) * (W - PAD_L - PAD_R)

    def sy(v):
        return H - PAD_B - (v - y0) / (y1 - y0) * (H - PAD_T - PAD_B)

    p = ['<?xml version="1.0" encoding="UTF-8"?>',
         '<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" '
         'viewBox="0 0 %d %d" font-family="DejaVu Sans, Helvetica, Arial, sans-serif">'
         % (W, H, W, H),
         '<rect width="%d" height="%d" fill="#ffffff"/>' % (W, H),
         '<text x="%d" y="26" font-size="16" font-weight="600" fill="#111827">%s</text>'
         % (PAD_L - 48, esc(title))]
    if subtitle:
        p.append('<text x="%d" y="44" font-size="11.5" fill="#6b7280">%s</text>'
                 % (PAD_L - 48, esc(subtitle)))

    # Horizontal grid + y tick labels.  `yticks` is a list of (position, label)
    # where position is already in axis space: on a log chart the caller passes
    # log10(value), because this function knows nothing about the transform and
    # placing raw values here put every decade in the same spot.
    for tv, tl in yticks:
        y = sy(tv)
        p.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="#e5e7eb" '
                 'stroke-width="1"/>' % (PAD_L, y, W - PAD_R, y))
        p.append('<text x="%.1f" y="%.1f" font-size="11" fill="#4b5563" '
                 'text-anchor="end">%s</text>' % (PAD_L - 9, y + 4, esc(tl)))

    # x tick labels
    for v, lab in xticks:
        x = sx(v)
        p.append('<text x="%.1f" y="%d" font-size="11" fill="#4b5563" '
                 'text-anchor="middle">%s</text>' % (x, H - PAD_B + 20, esc(lab)))

    # axes
    p.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#374151" stroke-width="1.4"/>'
             % (PAD_L, H - PAD_B, W - PAD_R, H - PAD_B))
    p.append('<line x1="%d" y1="%d" x2="%d" y2="%d" stroke="#374151" stroke-width="1.4"/>'
             % (PAD_L, PAD_T - 14, PAD_L, H - PAD_B))

    p.append(body)

    # axis titles
    p.append('<text x="%.1f" y="%d" font-size="12" fill="#374151" text-anchor="middle">%s</text>'
             % ((PAD_L + W - PAD_R) / 2, H - 30, esc(xlabel)))
    p.append('<text x="18" y="%.1f" font-size="12" fill="#374151" text-anchor="middle" '
             'transform="rotate(-90 18 %.1f)">%s</text>'
             % ((PAD_T + H - PAD_B) / 2, (PAD_T + H - PAD_B) / 2, esc(ylabel)))

    # legend along the bottom
    if legend:
        lx = PAD_L
        for i, (lab, col) in enumerate(legend):
            p.append('<rect x="%.1f" y="%d" width="22" height="9" rx="2" fill="%s"/>'
                     % (lx, H - 13, col))
            p.append('<text x="%.1f" y="%d" font-size="11" fill="#374151">%s</text>'
                     % (lx + 27, H - 5, esc(lab)))
            lx += 34 + 7.0 * len(lab)
    p.append('</svg>')
    return "\n".join(p)


def line_chart(path, title, subtitle, xlabel, ylabel, series, xcats, logy=False):
    """series: [(label, [y...], [yerr...])]; xcats: category labels on x."""
    n = len(xcats)
    x0, x1 = -0.35, n - 0.65
    allv = []
    for _, ys, es in series:
        for y, e in zip(ys, es):
            allv += [y + e, max(0.0, y - e)]
    if logy:
        allv = [v for v in allv if v > 0] or [1.0]
        lo, hi = math.log10(min(allv)), math.log10(max(allv))
        y0, y1 = math.floor(lo), math.ceil(hi)
        yticks = [(k, fmt(10.0 ** k)) for k in range(int(y0), int(y1) + 1)]

        def ty(v):
            return math.log10(v) if v > 0 else y0
    else:
        y0, y1, ticks = nice_ticks(0.0, max(allv) if allv else 1.0)
        yticks = [(t, fmt(t)) for t in ticks]

        def ty(v):
            return v

    def sx(i):
        return PAD_L + (i - x0) / (x1 - x0) * (W - PAD_L - PAD_R)

    def sy(v):
        return H - PAD_B - (ty(v) - y0) / (y1 - y0) * (H - PAD_T - PAD_B)

    body, legend = [], []
    for si, (lab, ys, es) in enumerate(series):
        col = PALETTE[si % len(PALETTE)]
        legend.append((lab, col))
        pts = " ".join("%.1f,%.1f" % (sx(i), sy(max(y, 1e-12) if logy else y))
                       for i, y in enumerate(ys))
        body.append('<polyline points="%s" fill="none" stroke="%s" stroke-width="2.4" '
                    'stroke-dasharray="%s" stroke-linejoin="round"/>'
                    % (pts, col, DASHES[si % len(DASHES)]))
        for i, (y, e) in enumerate(zip(ys, es)):
            x = sx(i)
            yv = max(y, 1e-12) if logy else y
            if e > 0:
                hi_v = y + e
                lo_v = max(y - e, 1e-12) if logy else max(y - e, 0.0)
                body.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                            'stroke-width="1.2" opacity="0.75"/>'
                            % (x, sy(hi_v), x, sy(lo_v), col))
                for cv in (hi_v, lo_v):
                    body.append('<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" stroke="%s" '
                                'stroke-width="1.2" opacity="0.75"/>'
                                % (x - 3.5, sy(cv), x + 3.5, sy(cv), col))
            body.append('<circle cx="%.1f" cy="%.1f" r="3.6" fill="%s" stroke="#ffffff" '
                        'stroke-width="1.2"/>' % (x, sy(yv), col))

    xticks = [(i, c) for i, c in enumerate(xcats)]
    svg = frame(title, subtitle, xlabel, ylabel, "\n".join(body), xticks, yticks,
                x0, x1, y0, y1, legend)
    write(path, svg)


def bar_chart(path, title, subtitle, xlabel, ylabel, series, xcats, logy=False):
    n, m = len(xcats), len(series)
    x0, x1 = -0.5, n - 0.5
    allv = [y for _, ys in series for y in ys]
    if logy:
        pos = [v for v in allv if v > 0] or [1.0]
        y0 = math.floor(math.log10(min(pos)))
        y1 = math.ceil(math.log10(max(pos)))
        yticks = [(k, fmt(10.0 ** k)) for k in range(int(y0), int(y1) + 1)]

        def ty(v):
            return math.log10(v) if v > 0 else y0
    else:
        y0, y1, ticks = nice_ticks(0.0, max(allv) if allv else 1.0)
        yticks = [(t, fmt(t)) for t in ticks]

        def ty(v):
            return v

    def sx(v):
        return PAD_L + (v - x0) / (x1 - x0) * (W - PAD_L - PAD_R)

    def sy(v):
        return H - PAD_B - (ty(v) - y0) / (y1 - y0) * (H - PAD_T - PAD_B)

    slot = (W - PAD_L - PAD_R) / n
    bw = slot * 0.74 / m
    body, legend = [], []
    base = sy(y0 if not logy else 10 ** y0)
    for si, (lab, ys) in enumerate(series):
        col = PALETTE[si % len(PALETTE)]
        legend.append((lab, col))
        for i, y in enumerate(ys):
            cx = sx(i) - slot * 0.37 + si * bw
            top = sy(y) if (y > 0 or not logy) else base
            h = max(0.0, base - top)
            body.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s" '
                        'rx="1.5"/>' % (cx, top, bw * 0.92, h, col))
            if y <= 0:
                body.append('<text x="%.1f" y="%.1f" font-size="9.5" fill="#6b7280" '
                            'text-anchor="middle">0</text>' % (cx + bw * 0.46, base - 3))
    xticks = [(i, c) for i, c in enumerate(xcats)]
    svg = frame(title, subtitle, xlabel, ylabel, "\n".join(body), xticks, yticks,
                x0, x1, y0, y1, legend)
    write(path, svg)


def write(path, svg):
    with open(path, "w", newline="\n", encoding="utf-8") as fh:
        fh.write(svg)
    print("  %s" % os.path.basename(path))


# ------------------------------------------------------------------ the graphs
def main():
    rows = load(SRC)
    if not rows:
        sys.exit("no usable rows in %s" % SRC)
    os.makedirs(OUT, exist_ok=True)
    out = lambda n: os.path.join(OUT, n)
    print("writing graphs:")

    def sel(**kw):
        return [r for r in rows if all(str(r.get(k)) == str(v) for k, v in kw.items())]

    # -- Part 1: aggregate transfer rate vs number of clients, both modes
    e1 = sel(exp="e1")
    if e1:
        ns = sorted({int(r["clients"]) for r in e1})
        topic = e1[0]["topic"]
        series, ov = [], []
        for mode in ("single", "multi"):
            ys, es, os_, oes = [], [], [], []
            for n in ns:
                g = [r for r in e1 if r["mode"] == mode and int(r["clients"]) == n]
                m, s = agg(g, "agg_goodput_mbps")
                ys.append(m); es.append(s)
                m2, s2 = agg(g, "overhead_pct")
                os_.append(m2); oes.append(s2)
            series.append(("%s-threaded" % mode, ys, es))
            ov.append(("%s-threaded" % mode, os_, oes))
        cats = [str(n) for n in ns]
        line_chart(out("baseline_throughput.svg"),
                   "Part I - aggregate transfer rate vs concurrent clients",
                   "topic '%s', loopback, mean of %d repeats, bars show +/-1 sd"
                   % (topic, len(e1) // (2 * len(ns)) or 1),
                   "concurrent clients", "aggregate rate (Mbit/s)", series, cats)
        line_chart(out("baseline_overhead.svg"),
                   "Part I - protocol overhead vs concurrent clients",
                   "overhead = (bytes on the wire - useful bytes) / useful bytes",
                   "concurrent clients", "overhead (%)", ov, cats)

    # -- Part 2: redundant bytes, Case 1 vs Case 2, across the fault rate
    e3 = sel(exp="e3")
    if e3:
        ps = sorted({r["p"] for r in e3}, key=float)
        topic = e3[0]["topic"]
        red, thr, ovh = [], [], []
        for case, lab in (("restart", "restart (no session)"),
                          ("resume", "resume (checkpointed)")):
            rs, ts, tes, os_, oes = [], [], [], [], []
            for p in ps:
                g = [r for r in e3 if r["case"] == case and r["p"] == p]
                rs.append(agg(g, lambda r: r["redundant_bytes"] / MB)[0])
                m, s = agg(g, "agg_goodput_mbps"); ts.append(m); tes.append(s)
                m2, s2 = agg(g, "overhead_pct"); os_.append(m2); oes.append(s2)
            red.append((lab, rs))
            thr.append((lab, ts, tes))
            ovh.append((lab, os_, oes))
        cats = [p for p in ps]
        bar_chart(out("restart_vs_resume_redundant.svg"),
                  "Part II - redundant bytes retransmitted vs fault rate",
                  "topic '%s'; p is per 16 KiB chunk; log scale - Case 2 is near zero"
                  % topic,
                  "failure probability p (per chunk)", "redundant bytes (MB, log)",
                  red, cats, logy=True)
        line_chart(out("restart_vs_resume_throughput.svg"),
                   "Part II - throughput vs fault rate",
                   "topic '%s', mean of repeats, bars show +/-1 sd" % topic,
                   "failure probability p (per chunk)", "goodput (Mbit/s)", thr, cats)
        line_chart(out("restart_vs_resume_overhead.svg"),
                   "Part II - protocol overhead vs fault rate",
                   "overhead = (bytes on the wire - useful bytes) / useful bytes",
                   "failure probability p (per chunk)", "overhead (%)", ovh, cats)

        # Per-case graphs under the naming convention the assignment gives
        # ("Partname_CaseName_throughput" / "_overhead"), so that every case
        # has its own pair in Results/ as well as the comparisons above.
        for case, stem, pretty in (
                ("restart", "restart", "restart - no session management"),
                ("resume", "resume", "resume - session management")):
            ts, tes, ov_, oves = [], [], [], []
            for p in ps:
                g = [r for r in e3 if r["case"] == case and r["p"] == p]
                m, s = agg(g, "agg_goodput_mbps")
                ts.append(m); tes.append(s)
                m2, s2 = agg(g, "overhead_pct")
                ov_.append(m2); oves.append(s2)
            line_chart(out("%s_throughput.svg" % stem),
                       "%s - throughput vs fault rate" % pretty,
                       "topic '%s', mean of repeats, bars show +/-1 sd" % topic,
                       "failure probability p (per chunk)", "goodput (Mbit/s)",
                       [("goodput", ts, tes)], cats)
            line_chart(out("%s_overhead.svg" % stem),
                       "%s - protocol overhead vs fault rate" % pretty,
                       "overhead = (bytes on the wire - useful bytes) / useful bytes",
                       "failure probability p (per chunk)", "overhead (%)",
                       [("overhead", ov_, oves)], cats)

    # -- Enhanced: throughput against K
    e5 = sel(exp="e5")
    e6 = sel(exp="e6")
    if e5:
        ks = sorted({int(r["K"]) for r in e5})
        series = []
        ys, es = [], []
        for k in ks:
            g = [r for r in e5 if int(r["K"]) == k]
            m, s = agg(g, "agg_goodput_mbps"); ys.append(m); es.append(s)
        series.append(("fault-free (p=0)", ys, es))
        if e6:
            ks6 = sorted({int(r["K"]) for r in e6})
            if ks6:
                y2 = []
                e2_ = []
                for k in ks:
                    g = [r for r in e6 if int(r["K"]) == k]
                    if g:
                        m, s = agg(g, "agg_goodput_mbps")
                    else:
                        m, s = 0.0, 0.0
                    y2.append(m); e2_.append(s)
                series.append(("with faults (p=%s)" % e6[0]["p"], y2, e2_))
        line_chart(out("parallel_throughput.svg"),
                   "Case 2 Enhanced - throughput vs parallel connections K",
                   "topic '%s', one client, K TCP connections multiplexed by one "
                   "server thread" % e5[0]["topic"],
                   "parallel connections K", "goodput (Mbit/s)", series,
                   [str(k) for k in ks])

        # The matching overhead graph, so the Enhanced case has the same pair
        # of graphs as the other cases.
        ov_, oves = [], []
        for k in ks:
            g = [r for r in e5 if int(r["K"]) == k]
            m, s = agg(g, "overhead_pct")
            ov_.append(m); oves.append(s)
        line_chart(out("parallel_overhead.svg"),
                   "Case 2 Enhanced - protocol overhead vs parallel connections K",
                   "the 12-byte (file_idx, abs_offset) prefix on every range chunk "
                   "is the cost; it does not vary with K",
                   "parallel connections K", "overhead (%)",
                   [("overhead", ov_, oves)], [str(k) for k in ks])

    # -- Part 1 vs every Part II case, fault-free, all on ONE topic (E7).
    # Restricted to E7 on purpose: the other experiments pick whichever topic
    # suits what they measure, so their p=0 rows are not comparable with each
    # other.  E7 exists to make this bar chart like-for-like.
    e7 = sel(exp="e7")
    if e7:
        groups = [("baseline", None, "baseline"), ("restart", None, "restart"),
                  ("resume", None, "resume"),
                  ("parallel", "1", "parallel K=1"),
                  ("parallel", "4", "parallel K=4")]
        cats, ys = [], []
        for case, k, lab in groups:
            g = [r for r in e7 if r["case"] == case
                 and (k is None or str(r["K"]) == k)]
            if not g:
                continue
            cats.append(lab)
            ys.append(agg(g, "agg_goodput_mbps")[0])
        if cats:
            bar_chart(out("faultfree_comparison_throughput.svg"),
                      "Fault-free throughput: Part I vs every Part II case",
                      "topic '%s', one client, p=0 - the standing cost of session "
                      "management" % e7[0]["topic"],
                      "implementation", "goodput (Mbit/s)",
                      [("p = 0, 1 client", ys)], cats)


def main_dist():
    """Graphs for the three-machine runs, if Results/distributed is present.

    These are kept separate from the loopback graphs because the two were
    measured in different environments and must never be averaged together.
    Where a figure shows both, it shows them as distinct series.
    """
    src = os.path.join(OUT, "distributed", "bench_runs_dist.csv")
    if not os.path.exists(src):
        return
    rows = load(src)
    if not rows:
        return
    loop = load(SRC) if os.path.exists(SRC) else []
    out = lambda n: os.path.join(OUT, n)
    print("writing three-machine graphs:")

    d1 = [r for r in rows if r["exp"] == "d1"]
    d2 = [r for r in rows if r["exp"] == "d2"]

    # -- Part I over the network: aggregate rate vs clients, both modes
    if d1:
        ns = sorted({int(r["clients"]) for r in d1})
        series = []
        for mode in ("single", "multi"):
            ys, es = [], []
            for n in ns:
                g = [r for r in d1 if r["mode"] == mode and int(r["clients"]) == n]
                m, s = agg(g, "agg_goodput_mbps")
                ys.append(m); es.append(s)
            series.append(("%s-threaded" % mode, ys, es))
        line_chart(out("baseline_3machine_throughput.svg"),
                   "Part I on three machines - aggregate rate vs clients",
                   "server on one host, clients split across two others over "
                   "802.11; topic '%s', mean of repeats, bars show +/-1 sd"
                   % d1[0]["topic"],
                   "concurrent clients", "aggregate rate (Mbit/s)", series,
                   [str(n) for n in ns])

        # -- the same advantage, loopback against the real network
        e1 = [r for r in loop if r["exp"] == "e1"]
        if e1:
            ratio_net, ratio_loop = [], []
            for n in ns:
                for src_rows, dst in ((d1, ratio_net), (e1, ratio_loop)):
                    s = agg([r for r in src_rows if r["mode"] == "single"
                             and int(r["clients"]) == n], "agg_goodput_mbps")[0]
                    m = agg([r for r in src_rows if r["mode"] == "multi"
                             and int(r["clients"]) == n], "agg_goodput_mbps")[0]
                    dst.append(m / s if s else 0.0)
            bar_chart(out("baseline_loopback_vs_3machine_advantage.svg"),
                      "Multi-threading advantage: loopback vs a real network",
                      "aggregate rate of the multi-threaded server divided by "
                      "the single-threaded one; 1.0 means no advantage",
                      "concurrent clients", "multi / single",
                      [("loopback (one host)", ratio_loop),
                       ("three machines (802.11)", ratio_net)],
                      [str(n) for n in ns])

    # -- Enhanced: K sweep, loopback against the real network
    if d2:
        ks = sorted({int(r["K"]) for r in d2})
        net, loopv = [], []
        e5 = [r for r in loop if r["exp"] == "e5"]
        for k in ks:
            net.append(agg([r for r in d2 if int(r["K"]) == k],
                           "agg_goodput_mbps")[0])
            loopv.append(agg([r for r in e5 if int(r["K"]) == k],
                             "agg_goodput_mbps")[0])
        base_n = net[0] or 1.0
        base_l = loopv[0] or 1.0
        bar_chart(out("parallel_3machine_speedup.svg"),
                  "Parallel connections: speedup vs K, loopback and a real network",
                  "goodput relative to K=1 in the same environment; topic '%s', "
                  "no faults" % d2[0]["topic"],
                  "parallel connections K", "speedup over K=1",
                  [("loopback (one host)", [v / base_l for v in loopv]),
                   ("three machines (802.11)", [v / base_n for v in net])],
                  [str(k) for k in ks])

        series = [("three machines (802.11)",
                   [agg([r for r in d2 if int(r["K"]) == k], "agg_goodput_mbps")[0]
                    for k in ks],
                   [agg([r for r in d2 if int(r["K"]) == k], "agg_goodput_mbps")[1]
                    for k in ks])]
        line_chart(out("parallel_3machine_throughput.svg"),
                   "Case 2 Enhanced on three machines - goodput vs K",
                   "one client opening K parallel connections across a real "
                   "wireless link; topic '%s'" % d2[0]["topic"],
                   "parallel connections K", "goodput (Mbit/s)", series,
                   [str(k) for k in ks])


if __name__ == "__main__":
    main()
    main_dist()
