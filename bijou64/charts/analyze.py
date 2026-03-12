#!/usr/bin/env python3
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "matplotlib>=3.8",
#   "numpy>=1.26",
#   "pandas>=2.1",
#   "plotly>=5.18",
#   "seaborn>=0.13",
# ]
# ///
"""Analyze Criterion benchmark results for the bijou64 shootout.

Reads target/criterion/**/new/{sample,estimates}.json, computes percentile
statistics, and generates:

  - bijou64/charts/percentiles.csv   (machine-readable summary)
  - bijou64/charts/percentiles.md    (markdown table)
  - bijou64/charts/*.svg             (static charts)
  - bijou64/charts/*.html            (interactive Plotly charts)

Usage:
  nix run .#bench-charts                         # via flake app
  uv run bijou64/charts/analyze.py               # via uv (auto-installs deps)
  python bijou64/charts/analyze.py               # with deps already installed

The script auto-detects the workspace root by looking for Cargo.toml.
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend for headless SVG generation

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import seaborn as sns
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Benchmark group prefixes we expect from shootout.rs.
# The criterion directory names follow the pattern: {group}_{distribution}
# e.g., "decode_tiny_0_247", "encode_small_248_65535"
BENCHMARK_GROUPS = [
    "canonical_decode",
    "decode",
    "encode",
    "encode_array",
    "encoded_size",
    "stream_decode",
]

LIBRARIES = ["bijou64", "leb128", "varu64", "vu128", "vu64"]

DISTRIBUTIONS = [
    "tiny_0_247",
    "small_248_65535",
    "medium_64k_4G",
    "large_above_4G",
    "boundary",
    "uniform_random",
]

# Short display names for distributions
DIST_LABELS = {
    "tiny_0_247": "tiny",
    "small_248_65535": "small",
    "medium_64k_4G": "medium",
    "large_above_4G": "large",
    "boundary": "boundary",
    "uniform_random": "uniform",
}

# Legacy names for backward compatibility with old Criterion data
DIST_ALIASES = {
    "tier_boundaries": "boundary",
}

def _dist_sort_key(dist: str) -> int:
    """Return sort index for a distribution, preserving DISTRIBUTIONS order."""
    try:
        return DISTRIBUTIONS.index(dist)
    except ValueError:
        return len(DISTRIBUTIONS)


# Accessible, colour-blind-friendly palette (matches gen_charts.py)
COLOURS = {
    "bijou64": "#16a34a",  # green
    "leb128": "#7c3aed",  # violet
    "varu64": "#d97706",  # amber/orange
    "vu128": "#2563eb",  # blue
    "vu64": "#dc2626",  # red
}

PERCENTILES = [50, 90, 95, 99, 99.9]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class BenchResult:
    """Parsed results for one benchmark (group + distribution + library)."""

    group: str
    distribution: str
    library: str
    per_iter_ns: np.ndarray  # per-iteration times in nanoseconds
    mean_ns: float
    mean_ci_lower: float
    mean_ci_upper: float
    median_ns: float
    std_dev_ns: float


def find_workspace_root() -> Path:
    """Walk up from CWD to find the workspace root (contains Cargo.toml with [workspace])."""
    p = Path.cwd()
    while p != p.parent:
        cargo = p / "Cargo.toml"
        if cargo.exists() and "[workspace]" in cargo.read_text():
            return p
        p = p.parent
    sys.exit("Could not find workspace root (no Cargo.toml with [workspace] found)")


def load_criterion_data(workspace: Path) -> list[BenchResult]:
    """Scan target/criterion/ for benchmark results."""
    criterion_dir = workspace / "target" / "criterion"
    if not criterion_dir.is_dir():
        sys.exit(
            f"No criterion data at {criterion_dir}\n"
            "Run: cargo bench -p bijou64 --bench shootout"
        )

    results: list[BenchResult] = []

    # Build reverse alias map: canonical name -> list of directory names to try
    _legacy_names: dict[str, list[str]] = {d: [d] for d in DISTRIBUTIONS}
    for old, new in DIST_ALIASES.items():
        if new in _legacy_names:
            _legacy_names[new].append(old)

    for group in BENCHMARK_GROUPS:
        for dist in DISTRIBUTIONS:
            # Try canonical name first, then legacy aliases
            bench_dir = None
            for dir_name in _legacy_names.get(dist, [dist]):
                candidate = criterion_dir / f"{group}_{dir_name}"
                if candidate.is_dir():
                    bench_dir = candidate
                    break
            if bench_dir is None:
                # encode_array uses a different naming convention:
                # encode_array/{library}/{dist} instead of encode_array_{dist}/{library}
                continue

            for lib in LIBRARIES:
                lib_dir = bench_dir / lib
                sample_file = lib_dir / "new" / "sample.json"
                estimates_file = lib_dir / "new" / "estimates.json"

                if not sample_file.exists():
                    continue

                sample = _load_json(sample_file)
                estimates = _load_json(estimates_file) if estimates_file.exists() else {}

                iters = np.array(sample["iters"])
                times = np.array(sample["times"])
                per_iter = times / iters  # nanoseconds per iteration

                est_mean = estimates.get("mean", {})
                est_median = estimates.get("median", {})
                est_sd = estimates.get("std_dev", {})
                ci = est_mean.get("confidence_interval", {})

                results.append(
                    BenchResult(
                        group=group,
                        distribution=dist,
                        library=lib,
                        per_iter_ns=per_iter,
                        mean_ns=est_mean.get("point_estimate", float(np.mean(per_iter))),
                        mean_ci_lower=ci.get("lower_bound", 0),
                        mean_ci_upper=ci.get("upper_bound", 0),
                        median_ns=est_median.get(
                            "point_estimate", float(np.median(per_iter))
                        ),
                        std_dev_ns=est_sd.get(
                            "point_estimate", float(np.std(per_iter))
                        ),
                    )
                )

    # Also check encode_array which uses criterion's two-level naming
    _load_two_level_group(criterion_dir, "encode_array", results)
    _load_two_level_group(criterion_dir, "encoded_size", results)

    if not results:
        sys.exit(
            "No benchmark data found.\n"
            "Run: cargo bench -p bijou64 --bench shootout"
        )

    return results


def _load_two_level_group(
    criterion_dir: Path, group: str, results: list[BenchResult]
) -> None:
    """Load benchmarks with two-level naming: group/library/distribution."""
    group_dir = criterion_dir / group
    if not group_dir.is_dir():
        return

    for lib in LIBRARIES:
        lib_dir = group_dir / lib
        if not lib_dir.is_dir():
            continue

        # Build list of (canonical_dist, dir_name) pairs to try
        _legacy_names: dict[str, list[str]] = {d: [d] for d in DISTRIBUTIONS}
        for old, new in DIST_ALIASES.items():
            if new in _legacy_names:
                _legacy_names[new].append(old)

        for dist in DISTRIBUTIONS:
            # Try canonical name first, then legacy aliases
            sample_file = None
            estimates_file = None
            for dir_name in _legacy_names.get(dist, [dist]):
                candidate = lib_dir / dir_name / "new" / "sample.json"
                if candidate.exists():
                    sample_file = candidate
                    estimates_file = lib_dir / dir_name / "new" / "estimates.json"
                    break

            if sample_file is None:
                continue

            # Skip if already loaded via flat naming
            if any(
                r.group == group and r.distribution == dist and r.library == lib
                for r in results
            ):
                continue

            sample = _load_json(sample_file)
            estimates = _load_json(estimates_file) if estimates_file is not None and estimates_file.exists() else {}

            iters = np.array(sample["iters"])
            times = np.array(sample["times"])
            per_iter = times / iters

            est_mean = estimates.get("mean", {})
            est_median = estimates.get("median", {})
            est_sd = estimates.get("std_dev", {})
            ci = est_mean.get("confidence_interval", {})

            results.append(
                BenchResult(
                    group=group,
                    distribution=dist,
                    library=lib,
                    per_iter_ns=per_iter,
                    mean_ns=est_mean.get("point_estimate", float(np.mean(per_iter))),
                    mean_ci_lower=ci.get("lower_bound", 0),
                    mean_ci_upper=ci.get("upper_bound", 0),
                    median_ns=est_median.get(
                        "point_estimate", float(np.median(per_iter))
                    ),
                    std_dev_ns=est_sd.get(
                        "point_estimate", float(np.std(per_iter))
                    ),
                )
            )


def _load_json(path: Path) -> dict[str, Any]:
    with open(path) as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------


@dataclass
class PercentileStats:
    """Computed percentile statistics for one benchmark."""

    group: str
    distribution: str
    library: str
    p50: float
    p90: float
    p95: float
    p99: float
    p999: float
    mean: float
    std_dev: float
    ci_lower: float
    ci_upper: float
    iqr: float
    cv: float  # coefficient of variation
    min: float
    max: float


def compute_percentiles(results: list[BenchResult]) -> list[PercentileStats]:
    """Compute percentile statistics from raw sample data."""
    stats: list[PercentileStats] = []
    for r in results:
        data = r.per_iter_ns
        p = np.percentile(data, PERCENTILES)
        q1, q3 = np.percentile(data, [25, 75])

        stats.append(
            PercentileStats(
                group=r.group,
                distribution=r.distribution,
                library=r.library,
                p50=p[0],
                p90=p[1],
                p95=p[2],
                p99=p[3],
                p999=p[4],
                mean=r.mean_ns,
                std_dev=r.std_dev_ns,
                ci_lower=r.mean_ci_lower,
                ci_upper=r.mean_ci_upper,
                iqr=float(q3 - q1),
                cv=r.std_dev_ns / r.mean_ns if r.mean_ns > 0 else 0,
                min=float(np.min(data)),
                max=float(np.max(data)),
            )
        )
    return stats


# ---------------------------------------------------------------------------
# Output: CSV + Markdown
# ---------------------------------------------------------------------------


def write_csv(stats: list[PercentileStats], out_dir: Path) -> None:
    """Write percentiles.csv."""
    path = out_dir / "percentiles.csv"
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "group",
                "distribution",
                "library",
                "p50_ns",
                "p90_ns",
                "p95_ns",
                "p99_ns",
                "p99.9_ns",
                "mean_ns",
                "std_dev_ns",
                "ci_lower_ns",
                "ci_upper_ns",
                "iqr_ns",
                "cv",
                "min_ns",
                "max_ns",
            ]
        )
        for s in sorted(stats, key=lambda s: (s.group, _dist_sort_key(s.distribution), s.library)):
            w.writerow(
                [
                    s.group,
                    s.distribution,
                    s.library,
                    f"{s.p50:.1f}",
                    f"{s.p90:.1f}",
                    f"{s.p95:.1f}",
                    f"{s.p99:.1f}",
                    f"{s.p999:.1f}",
                    f"{s.mean:.1f}",
                    f"{s.std_dev:.1f}",
                    f"{s.ci_lower:.1f}",
                    f"{s.ci_upper:.1f}",
                    f"{s.iqr:.1f}",
                    f"{s.cv:.4f}",
                    f"{s.min:.1f}",
                    f"{s.max:.1f}",
                ]
            )
    print(f"  wrote {path}")


def write_markdown(stats: list[PercentileStats], out_dir: Path) -> None:
    """Write percentiles.md with one table per benchmark group."""
    path = out_dir / "percentiles.md"
    lines: list[str] = [
        "# Shootout Percentiles",
        "",
        "> Auto-generated by `analyze.py` from Criterion sample data.",
        "> All times in nanoseconds per value (batch of 4096).",
        "",
    ]

    grouped: dict[str, list[PercentileStats]] = {}
    for s in stats:
        grouped.setdefault(s.group, []).append(s)

    for group in sorted(grouped):
        title = group.replace("_", " ").title()
        lines.append(f"## {title}")
        lines.append("")
        lines.append(
            "| Distribution | Library | p50 | p90 | p95 | p99 | p99.9 | mean | std dev | CV |"
        )
        lines.append(
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|"
        )

        group_stats = sorted(grouped[group], key=lambda s: (_dist_sort_key(s.distribution), s.library))
        for s in group_stats:
            dist = DIST_LABELS.get(s.distribution, s.distribution)
            lines.append(
                f"| {dist} | {s.library} "
                f"| {s.p50:.0f} | {s.p90:.0f} | {s.p95:.0f} "
                f"| {s.p99:.0f} | {s.p999:.0f} "
                f"| {s.mean:.0f} | {s.std_dev:.0f} | {s.cv:.3f} |"
            )
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))
    print(f"  wrote {path}")


# ---------------------------------------------------------------------------
# Charts: matplotlib/seaborn (SVG)
# ---------------------------------------------------------------------------


def _setup_style() -> None:
    """Configure matplotlib for clean, publication-quality charts."""
    sns.set_theme(style="whitegrid", font_scale=1.0)
    plt.rcParams.update(
        {
            "figure.dpi": 150,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.15,
            "font.family": "sans-serif",
            "font.sans-serif": ["Inter", "Helvetica Neue", "Arial"],
            "axes.titlesize": 13,
            "axes.labelsize": 11,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
        }
    )


def _ns_to_label(ns: float) -> tuple[float, str]:
    """Convert nanoseconds to a human-readable scale."""
    if ns >= 1_000_000:
        return ns / 1_000_000, "ms"
    if ns >= 1_000:
        return ns / 1_000, "\u00b5s"
    return ns, "ns"


def _auto_scale(values_ns: list[float]) -> tuple[float, str]:
    """Pick a scale factor and unit for a set of ns values."""
    if not values_ns:
        return 1.0, "ns"
    mx = max(values_ns)
    if mx >= 1_000_000:
        return 1_000_000, "ms"
    if mx >= 1_000:
        return 1_000, "\u00b5s"
    return 1, "ns"


def generate_box_plots(
    results: list[BenchResult], out_dir: Path
) -> None:
    """One box plot SVG per benchmark group."""
    _setup_style()

    by_group: dict[str, list[BenchResult]] = {}
    for r in results:
        by_group.setdefault(r.group, []).append(r)

    for group, group_results in sorted(by_group.items()):
        dists_present = sorted({r.distribution for r in group_results}, key=_dist_sort_key)
        libs_present = sorted({r.library for r in group_results})

        if not dists_present or not libs_present:
            continue

        # Collect all values for auto-scaling
        all_vals = []
        for r in group_results:
            all_vals.extend(r.per_iter_ns.tolist())
        scale_div, unit = _auto_scale(all_vals)

        n_dists = len(dists_present)
        fig, axes = plt.subplots(
            1, n_dists, figsize=(3 * n_dists, 4), sharey=True, squeeze=False
        )

        for i, dist in enumerate(dists_present):
            ax = axes[0][i]
            dist_results = [r for r in group_results if r.distribution == dist]
            dist_results.sort(key=lambda r: r.library)

            data = [r.per_iter_ns / scale_div for r in dist_results]
            labels = [r.library for r in dist_results]
            colors = [COLOURS.get(lib, "#999") for lib in labels]

            bp = ax.boxplot(
                data,
                patch_artist=True,
                widths=0.6,
                showfliers=True,
                flierprops={"markersize": 3, "alpha": 0.5},
                medianprops={"color": "#1e293b", "linewidth": 1.5},
                whiskerprops={"linewidth": 1},
                capprops={"linewidth": 1},
            )
            for patch, color in zip(bp["boxes"], colors):
                patch.set_facecolor(color)
                patch.set_alpha(0.7)

            ax.set_xticklabels(labels, rotation=45, ha="right")
            ax.set_title(DIST_LABELS.get(dist, dist), fontsize=10)
            if i == 0:
                ax.set_ylabel(f"Time ({unit})")

        fig.suptitle(
            f"{_group_title(group)} — Distribution of Per-Iteration Times",
            fontsize=13,
            fontweight="bold",
        )
        fig.tight_layout()

        path = out_dir / f"{group}_box.svg"
        fig.savefig(path, format="svg")
        plt.close(fig)
        print(f"  wrote {path}")


def generate_bar_charts(
    stats: list[PercentileStats], out_dir: Path
) -> None:
    """Grouped bar chart (median) with min\u2013p95 error whiskers, per group."""
    _setup_style()

    by_group: dict[str, list[PercentileStats]] = {}
    for s in stats:
        by_group.setdefault(s.group, []).append(s)

    for group, group_stats in sorted(by_group.items()):
        dists_present = sorted({s.distribution for s in group_stats}, key=_dist_sort_key)
        libs_present = sorted({s.library for s in group_stats})

        if not dists_present or not libs_present:
            continue

        # Auto-scale
        all_p95 = [s.p95 for s in group_stats]
        scale_div, unit = _auto_scale(all_p95)

        n_libs = len(libs_present)
        n_dists = len(dists_present)
        x = np.arange(n_dists)
        width = 0.8 / n_libs

        fig, ax = plt.subplots(figsize=(max(8, n_dists * 1.5), 5))

        for j, lib in enumerate(libs_present):
            medians = []
            err_low = []
            err_high = []
            for dist in dists_present:
                matching = [
                    s for s in group_stats
                    if s.distribution == dist and s.library == lib
                ]
                if matching:
                    s = matching[0]
                    med = s.p50 / scale_div
                    lo = (s.p50 - s.min) / scale_div
                    hi = (s.p95 - s.p50) / scale_div
                    medians.append(med)
                    err_low.append(max(0, lo))
                    err_high.append(max(0, hi))
                else:
                    medians.append(0)
                    err_low.append(0)
                    err_high.append(0)

            offset = (j - n_libs / 2 + 0.5) * width
            bars = ax.bar(
                x + offset,
                medians,
                width,
                label=lib,
                color=COLOURS.get(lib, "#999"),
                alpha=0.85,
                yerr=[err_low, err_high],
                error_kw={"lw": 1, "capsize": 2, "capthick": 1},
            )

        ax.set_xlabel("Distribution")
        ax.set_ylabel(f"Median time ({unit}), whiskers: min\u2013p95")
        ax.set_title(
            f"{_group_title(group)} — Median Latency by Library",
            fontweight="bold",
        )
        ax.set_xticks(x)
        ax.set_xticklabels(
            [DIST_LABELS.get(d, d) for d in dists_present], rotation=30, ha="right"
        )
        ax.legend(loc="best", framealpha=0.9)
        ax.set_ylim(bottom=0)

        fig.tight_layout()
        path = out_dir / f"{group}_bar.svg"
        fig.savefig(path, format="svg")
        plt.close(fig)
        print(f"  wrote {path}")


def generate_cdf_plots(
    results: list[BenchResult], out_dir: Path
) -> None:
    """CDF overlay SVG per benchmark group (one subplot per distribution)."""
    _setup_style()

    by_group: dict[str, list[BenchResult]] = {}
    for r in results:
        by_group.setdefault(r.group, []).append(r)

    for group, group_results in sorted(by_group.items()):
        dists_present = sorted({r.distribution for r in group_results}, key=_dist_sort_key)
        libs_present = sorted({r.library for r in group_results})

        if not dists_present or not libs_present:
            continue

        n_dists = len(dists_present)
        fig, axes = plt.subplots(
            1, n_dists, figsize=(4 * n_dists, 3.5), squeeze=False
        )

        for i, dist in enumerate(dists_present):
            ax = axes[0][i]
            dist_results = [r for r in group_results if r.distribution == dist]

            # Auto-scale for this distribution
            all_vals = []
            for r in dist_results:
                all_vals.extend(r.per_iter_ns.tolist())
            scale_div, unit = _auto_scale(all_vals)

            for r in sorted(dist_results, key=lambda r: r.library):
                sorted_data = np.sort(r.per_iter_ns / scale_div)
                cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
                ax.plot(
                    sorted_data,
                    cdf,
                    label=r.library,
                    color=COLOURS.get(r.library, "#999"),
                    linewidth=1.8,
                )

            ax.set_title(DIST_LABELS.get(dist, dist), fontsize=10)
            ax.set_xlabel(f"Time ({unit})")
            if i == 0:
                ax.set_ylabel("CDF")
            ax.set_ylim(0, 1.02)

            # Add horizontal reference lines at key percentiles
            for pct in [0.5, 0.9, 0.99]:
                ax.axhline(y=pct, color="#cbd5e1", linewidth=0.7, linestyle="--")

        # Single legend for the whole figure
        handles, labels = axes[0][0].get_legend_handles_labels()
        fig.legend(
            handles, labels, loc="upper center",
            ncol=len(libs_present), bbox_to_anchor=(0.5, 1.0),
            fontsize=9, framealpha=0.9,
        )

        fig.suptitle(
            f"{_group_title(group)} — CDF of Per-Iteration Times",
            fontsize=13, fontweight="bold", y=1.06,
        )
        fig.tight_layout()

        path = out_dir / f"{group}_cdf.svg"
        fig.savefig(path, format="svg")
        plt.close(fig)
        print(f"  wrote {path}")


def generate_heatmap(
    stats: list[PercentileStats], out_dir: Path
) -> None:
    """Heatmap SVG: library x distribution, color = median latency. One per group."""
    _setup_style()

    by_group: dict[str, list[PercentileStats]] = {}
    for s in stats:
        by_group.setdefault(s.group, []).append(s)

    for group, group_stats in sorted(by_group.items()):
        dists_present = sorted({s.distribution for s in group_stats}, key=_dist_sort_key)
        libs_present = sorted({s.library for s in group_stats})

        if not dists_present or not libs_present:
            continue

        # Build matrix
        matrix = np.full((len(libs_present), len(dists_present)), np.nan)
        for s in group_stats:
            i = libs_present.index(s.library)
            j = dists_present.index(s.distribution)
            matrix[i, j] = s.p50

        # Auto-scale
        valid = matrix[~np.isnan(matrix)]
        scale_div, unit = _auto_scale(valid.tolist()) if valid.size > 0 else (1, "ns")
        matrix_scaled = matrix / scale_div

        fig, ax = plt.subplots(figsize=(max(6, len(dists_present) * 1.2), max(3, len(libs_present) * 0.8)))

        im = ax.imshow(matrix_scaled, aspect="auto", cmap="YlOrRd")

        ax.set_xticks(range(len(dists_present)))
        ax.set_xticklabels(
            [DIST_LABELS.get(d, d) for d in dists_present], rotation=30, ha="right"
        )
        ax.set_yticks(range(len(libs_present)))
        ax.set_yticklabels(libs_present)

        # Annotate cells with values
        for i in range(len(libs_present)):
            for j in range(len(dists_present)):
                v = matrix_scaled[i, j]
                if not np.isnan(v):
                    text_color = "white" if v > np.nanmax(matrix_scaled) * 0.65 else "#1e293b"
                    ax.text(j, i, f"{v:.1f}", ha="center", va="center",
                            fontsize=8, color=text_color, fontweight="bold")

        cbar = fig.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label(f"Median ({unit})")

        ax.set_title(
            f"{_group_title(group)} — Median Latency Heatmap",
            fontweight="bold", pad=12,
        )
        fig.tight_layout()

        path = out_dir / f"{group}_heatmap.svg"
        fig.savefig(path, format="svg")
        plt.close(fig)
        print(f"  wrote {path}")


# ---------------------------------------------------------------------------
# Charts: Plotly (interactive HTML)
# ---------------------------------------------------------------------------


def generate_interactive_cdf(
    results: list[BenchResult], out_dir: Path
) -> None:
    """Interactive CDF HTML per group using Plotly."""
    by_group: dict[str, list[BenchResult]] = {}
    for r in results:
        by_group.setdefault(r.group, []).append(r)

    for group, group_results in sorted(by_group.items()):
        dists_present = sorted({r.distribution for r in group_results}, key=_dist_sort_key)
        libs_present = sorted({r.library for r in group_results})

        if not dists_present or not libs_present:
            continue

        fig = make_subplots(
            rows=1, cols=len(dists_present),
            subplot_titles=[DIST_LABELS.get(d, d) for d in dists_present],
            shared_yaxes=True,
            horizontal_spacing=0.04,
        )

        for col, dist in enumerate(dists_present, 1):
            dist_results = [r for r in group_results if r.distribution == dist]

            # Auto-scale
            all_vals = []
            for r in dist_results:
                all_vals.extend(r.per_iter_ns.tolist())
            scale_div, unit = _auto_scale(all_vals)

            for r in sorted(dist_results, key=lambda r: r.library):
                sorted_data = np.sort(r.per_iter_ns / scale_div)
                cdf = np.arange(1, len(sorted_data) + 1) / len(sorted_data)

                fig.add_trace(
                    go.Scatter(
                        x=sorted_data,
                        y=cdf,
                        name=r.library,
                        legendgroup=r.library,
                        showlegend=(col == 1),
                        line={"color": COLOURS.get(r.library, "#999"), "width": 2},
                        hovertemplate=f"{r.library}<br>Time: %{{x:.2f}} {unit}<br>CDF: %{{y:.3f}}<extra></extra>",
                    ),
                    row=1,
                    col=col,
                )

            fig.update_xaxes(title_text=f"Time ({unit})", row=1, col=col)

        fig.update_yaxes(title_text="CDF", row=1, col=1)
        fig.update_layout(
            title=f"{_group_title(group)} \u2014 CDF of Per-Iteration Times",
            height=450,
            width=400 * len(dists_present),
            template="plotly_white",
        )

        path = out_dir / f"{group}_cdf.html"
        fig.write_html(str(path), include_plotlyjs="cdn")
        print(f"  wrote {path}")


def generate_interactive_heatmap(
    stats: list[PercentileStats], out_dir: Path
) -> None:
    """Interactive heatmap HTML per group using Plotly."""
    by_group: dict[str, list[PercentileStats]] = {}
    for s in stats:
        by_group.setdefault(s.group, []).append(s)

    for group, group_stats in sorted(by_group.items()):
        dists_present = sorted({s.distribution for s in group_stats}, key=_dist_sort_key)
        libs_present = sorted({s.library for s in group_stats})

        if not dists_present or not libs_present:
            continue

        # Build matrix
        z = []
        hover_text = []
        for lib in libs_present:
            row = []
            hover_row = []
            for dist in dists_present:
                matching = [
                    s for s in group_stats
                    if s.distribution == dist and s.library == lib
                ]
                if matching:
                    s = matching[0]
                    row.append(s.p50)
                    hover_row.append(
                        f"{lib} / {DIST_LABELS.get(dist, dist)}<br>"
                        f"p50: {s.p50:.0f} ns<br>"
                        f"p90: {s.p90:.0f} ns<br>"
                        f"p95: {s.p95:.0f} ns<br>"
                        f"p99: {s.p99:.0f} ns<br>"
                        f"p99.9: {s.p999:.0f} ns<br>"
                        f"mean: {s.mean:.0f} ns"
                    )
                else:
                    row.append(None)
                    hover_row.append("")
            z.append(row)
            hover_text.append(hover_row)

        fig = go.Figure(
            data=go.Heatmap(
                z=z,
                x=[DIST_LABELS.get(d, d) for d in dists_present],
                y=libs_present,
                text=hover_text,
                hoverinfo="text",
                colorscale="YlOrRd",
                colorbar={"title": "Median (ns)"},
            )
        )

        fig.update_layout(
            title=f"{_group_title(group)} \u2014 Median Latency Heatmap",
            xaxis_title="Distribution",
            yaxis_title="Library",
            height=max(300, 60 * len(libs_present)),
            width=max(500, 100 * len(dists_present)),
            template="plotly_white",
        )

        path = out_dir / f"{group}_heatmap.html"
        fig.write_html(str(path), include_plotlyjs="cdn")
        print(f"  wrote {path}")


def generate_interactive_percentile_table(
    stats: list[PercentileStats], out_dir: Path
) -> None:
    """Single interactive HTML table with all percentile data, sortable."""
    rows = []
    for s in sorted(stats, key=lambda s: (s.group, s.distribution, s.library)):
        rows.append(
            {
                "Group": _group_title(s.group),
                "Distribution": DIST_LABELS.get(s.distribution, s.distribution),
                "Library": s.library,
                "p50 (ns)": f"{s.p50:.0f}",
                "p90 (ns)": f"{s.p90:.0f}",
                "p95 (ns)": f"{s.p95:.0f}",
                "p99 (ns)": f"{s.p99:.0f}",
                "p99.9 (ns)": f"{s.p999:.0f}",
                "Mean (ns)": f"{s.mean:.0f}",
                "Std Dev": f"{s.std_dev:.0f}",
                "CV": f"{s.cv:.3f}",
            }
        )

    df = pd.DataFrame(rows)

    # Write a standalone HTML with sortable table via DataTables
    html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>bijou64 Shootout Percentiles</title>
<link rel="stylesheet" href="https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css">
<script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
<script src="https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js"></script>
<style>
  body {{ font-family: Inter, 'Helvetica Neue', Arial, sans-serif; margin: 2em; }}
  h1 {{ color: #1e293b; }}
  table {{ font-size: 13px; }}
  td, th {{ padding: 6px 10px !important; }}
</style>
</head>
<body>
<h1>bijou64 Shootout &mdash; Percentile Statistics</h1>
<p>All times in nanoseconds per value (batch of 4096). Click column headers to sort.</p>
{df.to_html(index=False, table_id="percentiles", classes="display")}
<script>
$(document).ready(function() {{
    $('#percentiles').DataTable({{
        paging: false,
        order: [[0, 'asc'], [1, 'asc'], [2, 'asc']],
    }});
}});
</script>
</body>
</html>"""

    path = out_dir / "percentiles.html"
    with open(path, "w") as f:
        f.write(html)
    print(f"  wrote {path}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _group_title(group: str) -> str:
    """Pretty-print a benchmark group name."""
    return group.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    workspace = find_workspace_root()
    out_dir = workspace / "bijou64" / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Loading Criterion data...")
    results = load_criterion_data(workspace)
    print(f"  found {len(results)} benchmark results")

    print("\nComputing percentile statistics...")
    stats = compute_percentiles(results)

    print("\nWriting CSV + Markdown...")
    write_csv(stats, out_dir)
    write_markdown(stats, out_dir)

    print("\nGenerating static SVG charts...")
    generate_box_plots(results, out_dir)
    generate_bar_charts(stats, out_dir)
    generate_cdf_plots(results, out_dir)
    generate_heatmap(stats, out_dir)

    print("\nGenerating interactive HTML charts...")
    generate_interactive_cdf(results, out_dir)
    generate_interactive_heatmap(stats, out_dir)
    generate_interactive_percentile_table(stats, out_dir)

    print(f"\nDone. All output in {out_dir}/")


if __name__ == "__main__":
    main()
