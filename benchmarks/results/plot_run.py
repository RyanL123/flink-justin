#!/usr/bin/env python3
"""Plot benchmark run metrics from benchmarks/results.

Usage examples:
  python benchmarks/results/plot_run.py --query q8 --run-id 2026-02-24-kind-q8-a4s-justin-baseline
  python benchmarks/results/plot_run.py --query q8 --latest
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import MultipleLocator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Plot one benchmark run as a single image with all available time-series "
            "metrics in stacked subplots."
        )
    )
    parser.add_argument(
        "--query",
        required=True,
        help="Query directory name under nexmark (example: q8).",
    )
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "--run-id",
        help="Run id from runs.csv (example: 2026-02-24-kind-q8-a4s-justin-baseline).",
    )
    group.add_argument(
        "--latest",
        action="store_true",
        help="Use the last row in runs.csv for the selected query.",
    )
    parser.add_argument(
        "--results-root",
        default=str(Path(__file__).resolve().parent),
        help="Path to benchmarks/results root (defaults to this script's directory).",
    )
    parser.add_argument(
        "--output",
        help="Output image path (default: <query_dir>/<run_id>-plot.png).",
    )
    return parser.parse_args()


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Missing CSV file: {path}")

    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    return rows


def select_run_row(rows: list[dict[str, str]], run_id: str | None, latest: bool) -> dict[str, str]:
    if not rows:
        raise ValueError("runs.csv has no data rows.")

    if run_id:
        for row in rows:
            if row.get("run_id") == run_id:
                return row
        raise ValueError(f"run_id '{run_id}' not found in runs.csv.")

    if latest or not run_id:
        return rows[-1]

    raise ValueError("Could not resolve run selection.")


def find_samples_file(query_dir: Path, run_id: str) -> Path:
    exact = query_dir / f"{run_id}-samples.csv"
    if exact.exists():
        return exact

    candidates = sorted(query_dir.glob(f"*{run_id}*samples.csv"))
    if candidates:
        return candidates[0]

    raise FileNotFoundError(
        f"No sample CSV found for run_id '{run_id}' in {query_dir}. "
        f"Expected file like '{run_id}-samples.csv'."
    )


def detect_timestamp_column(columns: list[str]) -> str:
    priority = ["timestamp_epoch", "timestamp", "time", "t"]
    for name in priority:
        if name in columns:
            return name

    for column in columns:
        if "time" in column.lower():
            return column

    raise ValueError("Could not detect timestamp column in samples CSV.")


def to_float(value: str, column: str, index: int) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Non-numeric value in samples CSV at row {index + 1}, column '{column}': {value!r}"
        ) from exc


def configure_plot_style() -> None:
    plt.style.use("default")
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": "#B0B0B0",
            "axes.grid": True,
            "grid.color": "#E6E6E6",
            "grid.linestyle": "-",
            "grid.linewidth": 0.7,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 12,
            "legend.frameon": False,
            "lines.linewidth": 1.8,
        }
    )


def pretty_metric_label(column: str) -> str:
    label = column.replace("_", " ").strip()
    label = " ".join(word.capitalize() for word in label.split())
    return label


def convert_units(column: str, values: list[float]) -> tuple[list[float], str]:
    lower = column.lower()
    if "bytes" in lower:
        return [v / (1024.0 * 1024.0) for v in values], "MiB"
    if "per_sec" in lower or "per_second" in lower:
        return values, "/s"
    return values, ""


def infer_axis_limits(metric_name: str, values: list[float]) -> tuple[float, float]:
    """Return per-run axis limits with light padding for readability."""
    if not values:
        return 0.0, 1.0

    lower_name = metric_name.lower()
    min_v = min(values)
    max_v = max(values)

    if math.isclose(min_v, max_v):
        if math.isclose(max_v, 0.0):
            return 0.0, 1.0
        pad = abs(max_v) * 0.10
        return min_v - pad, max_v + pad

    span = max_v - min_v
    pad = span * 0.08

    if "slot" in lower_name and "used" in lower_name:
        upper = math.ceil((max_v + 0.2) / 1.0) * 1.0
        return 0.0, max(1.0, upper)

    return min_v - pad, max_v + pad


def build_series(samples: list[dict[str, str]]) -> tuple[list[float], dict[str, list[float]]]:
    if not samples:
        raise ValueError("Sample CSV is empty.")

    columns = list(samples[0].keys())
    timestamp_column = detect_timestamp_column(columns)
    metric_columns = [c for c in columns if c != timestamp_column]
    if not metric_columns:
        raise ValueError("No metric columns found in samples CSV.")

    timestamps = [to_float(row[timestamp_column], timestamp_column, i) for i, row in enumerate(samples)]
    t0 = timestamps[0]
    elapsed_seconds = [ts - t0 for ts in timestamps]

    series: dict[str, list[float]] = {}
    for column in metric_columns:
        series[column] = [to_float(row[column], column, i) for i, row in enumerate(samples)]

    return elapsed_seconds, series


def plot_run(
    query: str,
    run_row: dict[str, str],
    elapsed_seconds: list[float],
    series: dict[str, list[float]],
    output_path: Path,
) -> None:
    configure_plot_style()

    metric_names = list(series.keys())
    figure, axes = plt.subplots(
        nrows=len(metric_names),
        ncols=1,
        figsize=(12, max(3.8 * len(metric_names), 6.0)),
        sharex=True,
        constrained_layout=False,
    )
    if len(metric_names) == 1:
        axes = [axes]

    colors = ["#1f77b4", "#d62728", "#2ca02c", "#9467bd", "#ff7f0e", "#17becf"]

    for i, (ax, metric_name) in enumerate(zip(axes, metric_names)):
        raw_values = series[metric_name]
        values, unit = convert_units(metric_name, raw_values)
        label = pretty_metric_label(metric_name)
        if unit:
            label = f"{label} ({unit})"

        ax.plot(
            elapsed_seconds,
            values,
            color=colors[i % len(colors)],
            marker="o",
            markersize=3.5,
        )
        ax.set_ylabel(label)
        ymin, ymax = infer_axis_limits(metric_name, values)
        ax.set_ylim(ymin, ymax)

        if "slot" in metric_name.lower() and "used" in metric_name.lower():
            ax.yaxis.set_major_locator(MultipleLocator(1.0))

    axes[-1].set_xlabel("Elapsed time (s)")

    run_id = run_row.get("run_id", "unknown-run")
    autoscaler = run_row.get("autoscaler", "unknown")
    run_commit = run_row.get("run_commit", "not-captured")
    sample_count = str(len(elapsed_seconds))
    title = f"{query.upper()} - {run_id}"
    subtitle = f"Autoscaler: {autoscaler} | Commit: {run_commit} | Samples: {sample_count}"
    figure.suptitle(f"{title}\n{subtitle}", y=0.97)
    figure.subplots_adjust(top=0.84, hspace=0.35)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=300, bbox_inches="tight")
    plt.close(figure)


def main() -> None:
    args = parse_args()
    results_root = Path(args.results_root).resolve()
    query_dir = results_root / "nexmark" / args.query
    runs_csv = query_dir / "runs.csv"

    run_rows = read_csv_rows(runs_csv)
    run_row = select_run_row(run_rows, args.run_id, args.latest)
    run_id = run_row.get("run_id")
    if not run_id:
        raise ValueError("Selected runs.csv row has no run_id.")

    samples_path = find_samples_file(query_dir, run_id)
    samples = read_csv_rows(samples_path)
    elapsed_seconds, series = build_series(samples)

    if args.output:
        output_path = Path(args.output).resolve()
    else:
        output_path = query_dir / f"{run_id}-plot.png"

    plot_run(args.query, run_row, elapsed_seconds, series, output_path)
    print(f"Saved plot: {output_path}")


if __name__ == "__main__":
    main()
