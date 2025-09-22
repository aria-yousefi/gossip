#!/usr/bin/env python3
"""
Run end-to-end experiments for gossip/push-sum on multiple topologies and sizes.

This script:
  1) Invokes your Gleam app: `gleam run <n> <topology> <algorithm> <trial> csv quiet`
  2) Parses a single RESULT line, e.g.:
     RESULT,algorithm=gossip,topology=grid3d,n=512,trial=1,convergence_ms=734
  3) Appends to results.csv
  4) Aggregates (mean ± std) by (algorithm, topology, n)
  5) Plots two figures (gossip_vs_n.png, pushsum_vs_n.png)
  6) Creates Report.pdf with both figures embedded

Quick start:
  python run_experiments.py
  python run_experiments.py --trials 5 --topos full line grid3d imperfect3d --algs gossip push-sum

Customize defaults in CONFIG below or via CLI flags (see code).
"""

import argparse
import csv
import math
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

# --------------------------
# CONFIG (edit as desired)
# --------------------------

DEFAULT_TOPOS = ["full", "line", "grid3d", "imperfect3d"]
DEFAULT_ALGS = ["gossip", "push-sum"]

# Sizes to sweep by topology (you can override via CLI)
DEFAULT_SIZES_FULL_LINE = [32, 64, 128, 256, 512, 1024]
DEFAULT_SIZES_GRID = [27, 64, 125, 216, 343, 512]  # ~ L^3

DEFAULT_TRIALS = 5

RESULTS_CSV = "results.csv"
GOSSIP_PNG = "gossip_vs_n.png"
PUSHSUM_PNG = "pushsum_vs_n.png"
REPORT_PDF = "Report.pdf"

GLEAM_CMD = ["gleam", "run"]  # customize if needed
EXTRA_FLAGS = ["csv", "quiet"]  # appended to every run

TIMEOUT_SEC = 600  # per run timeout safety

# --------------------------
# Helpers
# --------------------------

RESULT_RE = re.compile(
    r"^RESULT,algorithm=(?P<algorithm>[^,]+),topology=(?P<topology>[^,]+),n=(?P<n>\d+),trial=(?P<trial>\d+),convergence_ms=(?P<ms>\d+)\s*$"
)

@dataclass
class RunResult:
    algorithm: str
    topology: str
    n: int
    trial: int
    ms: int

def parse_result_line(line: str) -> RunResult:
    m = RESULT_RE.match(line.strip())
    if not m:
        raise ValueError(f"Could not parse RESULT line: {line!r}")
    return RunResult(
        algorithm=m.group("algorithm"),
        topology=m.group("topology"),
        n=int(m.group("n")),
        trial=int(m.group("trial")),
        ms=int(m.group("ms")),
    )

def run_one_case(n: int, topo: str, alg: str, trial: int,
                 gleam_cmd: List[str], extra_flags: List[str],
                 timeout_sec: int) -> RunResult:
    """
    Call: gleam run <n> <topology> <algorithm> <trial> csv quiet
    Capture stdout, parse RESULT line.
    """
    cmd = list(gleam_cmd) + [str(n), topo, alg, str(trial)] + extra_flags
    start = time.time()
    try:
        proc = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                timeout=timeout_sec,
                text=True,
                check=False,
    )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"Timed out: {' '.join(cmd)}")

    out = proc.stdout or ""
    # Keep a local log per run (optional)
    # open(f"log_{alg}_{topo}_{n}_{trial}.txt", "w", encoding="utf-8").write(out)

    # Look for RESULT line
    for line in out.splitlines():
        if line.startswith("RESULT,"):
            return parse_result_line(line)

    # If not found, show output snippet to help debug
    snippet = "\n".join(out.splitlines()[-20:])
    raise RuntimeError(
        f"No RESULT line found for {cmd}\n"
        f"Exit code: {proc.returncode}\nLast output lines:\n{snippet}"
    )

def append_to_csv(path: str, rows: List[RunResult]) -> None:
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(["algorithm", "topology", "n", "trial", "convergence_ms"])
        for r in rows:
            w.writerow([r.algorithm, r.topology, r.n, r.trial, r.ms])

def load_csv(path: str) -> List[RunResult]:
    if not os.path.exists(path):
        return []
    out: List[RunResult] = []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        for row in rdr:
            out.append(
                RunResult(
                    algorithm=row["algorithm"],
                    topology=row["topology"],
                    n=int(row["n"]),
                    trial=int(row["trial"]),
                    ms=int(row["convergence_ms"]),
                )
            )
    return out

def aggregate_stats(results: List[RunResult]) -> Dict[Tuple[str, str], Dict[int, Tuple[float, float]]]:
    """
    Returns stats[(algorithm, topology)][n] = (mean_ms, std_ms)
    """
    buckets: Dict[Tuple[str, str, int], List[int]] = defaultdict(list)
    for r in results:
        buckets[(r.algorithm, r.topology, r.n)].append(r.ms)

    stats: Dict[Tuple[str, str], Dict[int, Tuple[float, float]]] = defaultdict(dict)
    for (alg, topo, n), vals in buckets.items():
        m = sum(vals) / len(vals)
        var = sum((v - m) ** 2 for v in vals) / max(1, len(vals) - 1)
        sd = math.sqrt(var)
        stats[(alg, topo)][n] = (m, sd)
    return stats

def plot_for_algorithm(stats, alg: str, out_png: str) -> None:
    topologies = ["full", "line", "grid3d", "imperfect3d"]
    style = {
        "full": {"marker": "o"},
        "line": {"marker": "s"},
        "grid3d": {"marker": "^"},
        "imperfect3d": {"marker": "D"},
    }
    present = False
    plt.figure(figsize=(7, 5))
    for topo in topologies:
        series = stats.get((alg, topo), {})
        if not series:
            continue
        Ns = sorted(series.keys())
        means = [series[n][0] for n in Ns]
        sds = [series[n][1] for n in Ns]
        plt.errorbar(Ns, means, yerr=sds, capsize=3, label=topo, **style.get(topo, {}))
        present = True

    if not present:
        plt.close()
        return

    plt.xscale("log")
    # Uncomment if convergence times vary by orders of magnitude:
    # plt.yscale("log")
    plt.xlabel("Network size N (log scale)")
    plt.ylabel("Convergence time (ms)")
    plt.title(f"{alg.capitalize()} convergence vs N")
    plt.grid(True, which="both", ls=":", alpha=0.5)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close()

def make_report_pdf(pngs: List[Tuple[str, str]], out_pdf: str) -> None:
    with PdfPages(out_pdf) as pdf:
        for fn, title in pngs:
            if not os.path.exists(fn):
                continue
            fig = plt.figure(figsize=(8.5, 6))
            img = plt.imread(fn)
            plt.imshow(img)
            plt.axis("off")
            plt.title(title)
            pdf.savefig(fig)
            plt.close(fig)

def brief_console_findings(stats) -> None:
    print("\nQuick findings (rough growth across your size range):")
    for alg in ["gossip", "push-sum"]:
        for topo in ["full", "line", "grid3d", "imperfect3d"]:
            series = stats.get((alg, topo), {})
            if not series:
                continue
            Ns = sorted(series.keys())
            if len(Ns) >= 2:
                first, last = Ns[0], Ns[-1]
                grow = stats[(alg, topo)][last][0] / max(1e-9, stats[(alg, topo)][first][0])
                print(f"  {alg:8s} {topo:12s}: ~x{grow:.1f} from N={first} to {last}")

# --------------------------
# Main
# --------------------------

def main():
    p = argparse.ArgumentParser(description="Run Gleam topology/algorithm experiments and plot results.")
    p.add_argument("--topos", nargs="*", default=DEFAULT_TOPOS,
                   help="Topologies to run (default: full line grid3d imperfect3d)")
    p.add_argument("--algs", nargs="*", default=DEFAULT_ALGS,
                   help="Algorithms to run (default: gossip push-sum)")
    p.add_argument("--trials", type=int, default=DEFAULT_TRIALS, help="Trials per (alg, topo, n)")
    p.add_argument("--sizes_full_line", nargs="*", type=int, default=DEFAULT_SIZES_FULL_LINE,
                   help="Sizes for full/line (default: 32 64 128 256 512 1024)")
    p.add_argument("--sizes_grid", nargs="*", type=int, default=DEFAULT_SIZES_GRID,
                   help="Sizes for grid3d/imperfect3d (default: 27 64 125 216 343 512)")
    p.add_argument("--results_csv", default=RESULTS_CSV, help="CSV output path")
    p.add_argument("--gleam_cmd", nargs="*", default=GLEAM_CMD, help="Command to run Gleam app (default: gleam run)")
    p.add_argument("--extra_flags", nargs="*", default=EXTRA_FLAGS, help='Extra flags passed to app (default: "csv quiet")')
    p.add_argument("--skip_runs", action="store_true", help="Skip running Gleam (just plot from existing CSV)")
    p.add_argument("--timeout", type=int, default=TIMEOUT_SEC, help="Per-run timeout seconds")
    args = p.parse_args()

    # 1) Run sweeps (unless skipping)
    new_rows: List[RunResult] = []

    if not args.skip_runs:
        for alg in args.algs:
            for topo in args.topos:
                sizes = args.sizes_grid if topo in ("grid3d", "imperfect3d") else args.sizes_full_line
                for n in sizes:
                    for trial in range(1, args.trials + 1):
                        print(f"Running: alg={alg} topo={topo} n={n} trial={trial}")
                        try:
                            res = run_one_case(n, topo, alg, trial, args.gleam_cmd, args.extra_flags, args.timeout)
                        except Exception as e:
                            print(f"  FAILED: {e}", file=sys.stderr)
                            continue
                        print(f"  -> {res.ms} ms")
                        new_rows.append(res)

        if new_rows:
            append_to_csv(args.results_csv, new_rows)
            print(f"Appended {len(new_rows)} rows to {args.results_csv}")

    # 2) Load all results
    all_rows = load_csv(args.results_csv)
    if not all_rows:
        print("No results found. Run without --skip_runs first.")
        return

    # 3) Aggregate and plot
    stats = aggregate_stats(all_rows)
    plot_for_algorithm(stats, "gossip", GOSSIP_PNG)
    plot_for_algorithm(stats, "push-sum", PUSHSUM_PNG)

    # 4) Report
    make_report_pdf(
        [(GOSSIP_PNG, "Gossip – Convergence vs N"),
         (PUSHSUM_PNG, "Push-Sum – Convergence vs N")],
        REPORT_PDF,
    )

    # 5) Findings
    brief_console_findings(stats)
    print(f"\nWrote: {args.results_csv}, {GOSSIP_PNG}, {PUSHSUM_PNG}, {REPORT_PDF}")

if __name__ == "__main__":
    main()
