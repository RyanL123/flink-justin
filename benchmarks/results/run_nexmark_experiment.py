#!/usr/bin/env python3
"""Run one Nexmark query experiment and persist comparable results.

This script:
1) applies a FlinkDeployment manifest
2) waits for the job to run
3) samples canonical metrics from Flink REST at a fixed interval
4) deletes the deployment
5) appends a normalized summary row to runs.csv
6) optionally generates a plot via plot_run.py
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import subprocess
import sys
import time
import urllib.parse
from pathlib import Path


RUNS_HEADER = ["run_id", "environment", "run_commit", "autoscaler"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one Nexmark query benchmark and save samples, summary, and plot."
    )
    parser.add_argument("--query", required=True, help="Query directory name, e.g. q1, q2, q11.")
    parser.add_argument(
        "--manifest",
        help="Path to FlinkDeployment YAML (default: notebooks/nexmark/<query>/query<num>.yaml).",
    )
    parser.add_argument(
        "--duration-sec",
        type=int,
        default=600,
        help="Total sampling duration in seconds (default: 600).",
    )
    parser.add_argument(
        "--sampling-interval-sec",
        type=int,
        default=5,
        help="Sampling interval in seconds (default: 5).",
    )
    parser.add_argument(
        "--environment",
        default="kind",
        help="Environment tag used in run_id and runs.csv (default: kind).",
    )
    parser.add_argument(
        "--policy",
        default="a4s-justin",
        help="Policy tag used in run_id (default: a4s-justin).",
    )
    parser.add_argument(
        "--results-root",
        default=str(Path(__file__).resolve().parent),
        help="Path to benchmarks/results root (default: this script's directory).",
    )
    parser.add_argument(
        "--wait-running-timeout-sec",
        type=int,
        default=600,
        help="Timeout waiting for RUNNING job (default: 600).",
    )
    parser.add_argument(
        "--cleanup-timeout-sec",
        type=int,
        default=240,
        help="Timeout waiting for FlinkDeployment deletion (default: 240).",
    )
    parser.add_argument(
        "--plot",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Generate plot after run (default: true).",
    )
    return parser.parse_args()


def shell(command: str, cwd: Path, check: bool = True) -> tuple[str, int]:
    proc = subprocess.run(
        command,
        shell=True,
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {command}\n{proc.stdout}")
    return proc.stdout.strip(), proc.returncode


def slugify(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-").lower() or "run"


def default_manifest_for_query(repo_root: Path, query: str) -> Path:
    match = re.fullmatch(r"q(\d+)", query)
    if not match:
        raise ValueError(f"Cannot infer manifest for query '{query}'. Provide --manifest.")
    qnum = match.group(1)
    return repo_root / f"notebooks/nexmark/{query}/query{qnum}.yaml"


def get_json_via_kubectl_raw(repo_root: Path, base_path: str, path: str, retries: int = 4) -> dict:
    raw_path = f"{base_path}{path}"
    last_output = ""
    for i in range(retries):
        proc = subprocess.run(
            f"kubectl get --raw '{raw_path}'",
            shell=True,
            cwd=repo_root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if proc.returncode == 0:
            return json.loads(proc.stdout)
        last_output = (proc.stderr or proc.stdout).strip()
        time.sleep(1 + i)
    raise RuntimeError(f"kubectl get --raw failed for {path}: {last_output}")


def wait_no_deployment(repo_root: Path, timeout_sec: int) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        out, code = shell("kubectl get flinkdeployment flink -o name", repo_root, check=False)
        if code != 0 or "NotFound" in out or out == "":
            return
        time.sleep(2)
    raise TimeoutError("Timed out waiting for flinkdeployment cleanup.")


def wait_job_running(repo_root: Path, timeout_sec: int) -> str:
    start = time.time()
    while time.time() - start < timeout_sec:
        state, _ = shell(
            "kubectl get flinkdeployment flink -o jsonpath='{.status.jobStatus.state}'",
            repo_root,
            check=False,
        )
        job_id, _ = shell(
            "kubectl get flinkdeployment flink -o jsonpath='{.status.jobStatus.jobId}'",
            repo_root,
            check=False,
        )
        if state == "RUNNING" and job_id:
            return job_id
        time.sleep(3)
    raise TimeoutError("Timed out waiting for RUNNING job.")


def wait_rest_ready(repo_root: Path, base_path: str, timeout_sec: int = 180) -> None:
    start = time.time()
    while time.time() - start < timeout_sec:
        try:
            overview = get_json_via_kubectl_raw(repo_root, base_path, "/overview", retries=1)
            if "flink-version" in overview:
                return
        except Exception:
            pass
        time.sleep(2)
    raise TimeoutError("Timed out waiting for Flink REST readiness.")


def get_source_vertex_ids(repo_root: Path, base_path: str, job_id: str) -> tuple[list[str], dict[str, str]]:
    job = get_json_via_kubectl_raw(repo_root, base_path, f"/jobs/{job_id}")
    vertices = job.get("vertices", [])
    source_vertices = [v for v in vertices if "Source:" in v.get("name", "")]
    if not source_vertices:
        source_vertices = [v for v in vertices if "source" in v.get("name", "").lower()]

    source_ids = [v["id"] for v in source_vertices]
    source_names = {v["id"]: v.get("name", "<unknown>") for v in source_vertices}

    print("Detected source vertices:")
    for v in source_vertices:
        print(
            f"  - id={v.get('id')} "
            f"name={v.get('name', '<unknown>')} "
            f"parallelism={v.get('parallelism', '<unknown>')}"
        )
    if not source_vertices:
        print("  - none detected")

    return source_ids, source_names


def sample_once(
    repo_root: Path,
    base_path: str,
    job_id: str,
    source_vids: list[str],
    source_names: dict[str, str],
) -> tuple[int, float, float, float]:
    ts = int(time.time())
    overview = get_json_via_kubectl_raw(repo_root, base_path, "/overview")
    slots_used = float(int(overview.get("slots-total", 0)) - int(overview.get("slots-available", 0)))

    mem_used = 0.0
    for tm in get_json_via_kubectl_raw(repo_root, base_path, "/taskmanagers").get("taskmanagers", []):
        tm_id = tm["id"]
        vals = get_json_via_kubectl_raw(
            repo_root,
            base_path,
            f"/taskmanagers/{tm_id}/metrics?get=Status.Flink.Memory.Managed.Used",
            retries=2,
        )
        if vals:
            try:
                mem_used += float(vals[0]["value"])
            except Exception:
                pass

    throughput = 0.0
    for vid in source_vids:
        source_name = source_names.get(vid, "<unknown>")
        metrics = get_json_via_kubectl_raw(repo_root, base_path, f"/jobs/{job_id}/vertices/{vid}/metrics", retries=2)
        # Flink may expose multiple scoped metrics per subtask for chained operators
        # (e.g. "<subtask>.Map.numRecordsOutPerSecond", "<subtask>.Timestamps/...").
        # Count exactly one source-out metric per subtask to avoid duplicate summation.
        metric_ids = sorted(
            {
                m["id"]
                for m in metrics
                if re.fullmatch(r"\d+\.numRecordsOutPerSecond", m.get("id", ""))
            },
            key=lambda mid: int(mid.split(".", 1)[0]),
        )
        if not metric_ids:
            print(f"[metrics] ts={ts} source={source_name} ({vid}) numRecordsOutPerSecond metrics=none")
            continue
        print(
            f"[metrics] ts={ts} source={source_name} ({vid}) "
            f"numRecordsOutPerSecond metric_ids={','.join(metric_ids)}"
        )
        encoded = urllib.parse.quote(",".join(metric_ids), safe=",")
        vals = get_json_via_kubectl_raw(
            repo_root,
            base_path,
            f"/jobs/{job_id}/vertices/{vid}/metrics?get={encoded}",
            retries=2,
        )
        for item in vals:
            try:
                metric_value = float(item["value"])
                throughput += metric_value
                print(
                    f"[metrics] ts={ts} source={source_name} ({vid}) "
                    f"metric={item.get('id', '<unknown>')} value={metric_value}"
                )
            except Exception:
                pass

    print(
        f"[metrics] ts={ts} totals "
        f"source_throughput_records_per_sec={throughput} "
        f"total_managed_memory_used_bytes={mem_used} "
        f"total_slots_used={slots_used}"
    )

    return ts, throughput, mem_used, slots_used


def append_run_row(runs_csv: Path, row: dict[str, str]) -> None:
    exists = runs_csv.exists()
    existing_rows: list[dict[str, str]] = []
    if exists:
        with runs_csv.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames != RUNS_HEADER:
                raise ValueError(
                    f"Unsupported runs.csv header in {runs_csv}. "
                    "Expected header: run_id,environment,run_commit,autoscaler."
                )
            existing_rows = list(reader)
        if any(r.get("run_id") == row["run_id"] for r in existing_rows):
            return

    with runs_csv.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=RUNS_HEADER)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def build_run_row(
    args: argparse.Namespace,
    run_id: str,
    run_commit: str,
) -> dict[str, str]:
    return {
        "run_id": run_id,
        "environment": args.environment,
        "run_commit": run_commit,
        "autoscaler": slugify(args.policy).split("-", 1)[0],
    }


def main() -> None:
    args = parse_args()
    if args.duration_sec <= 0:
        raise ValueError("--duration-sec must be > 0")
    if args.sampling_interval_sec <= 0:
        raise ValueError("--sampling-interval-sec must be > 0")

    repo_root = Path(__file__).resolve().parents[2]
    results_root = Path(args.results_root).resolve()
    query = slugify(args.query)
    manifest = Path(args.manifest).resolve() if args.manifest else default_manifest_for_query(repo_root, query)
    if not manifest.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest}")

    run_id = str(int(time.time() * 1000))
    autoscaler = slugify(args.policy).split("-", 1)[0]
    run_name = f"{run_id}_{slugify(args.environment)}_{slugify(autoscaler)}"
    query_dir = results_root / "nexmark" / query
    query_dir.mkdir(parents=True, exist_ok=True)
    runs_csv = query_dir / "runs.csv"
    run_dir = query_dir / run_name
    run_dir.mkdir(parents=True, exist_ok=True)
    samples_csv = run_dir / "samples.csv"

    base_path = "/api/v1/namespaces/default/services/http:flink-rest:8081/proxy"
    sample_count = max(1, math.ceil(args.duration_sec / args.sampling_interval_sec))

    print(f"Starting run_id: {run_id}")
    print(f"Run folder: {run_dir}")
    print(f"Manifest: {manifest}")
    print(f"Sampling: {sample_count} samples at {args.sampling_interval_sec}s interval")

    shell("kubectl delete flinkdeployment flink --ignore-not-found=true", repo_root, check=False)
    wait_no_deployment(repo_root, args.cleanup_timeout_sec)

    rows: list[tuple[int, float, float, float]] = []
    job_id = "not-captured"
    try:
        shell(f"kubectl apply -f '{manifest}'", repo_root)
        job_id = wait_job_running(repo_root, args.wait_running_timeout_sec)
        wait_rest_ready(repo_root, base_path)
        vids, source_names = get_source_vertex_ids(repo_root, base_path, job_id)

        for i in range(sample_count):
            t0 = time.time()
            rows.append(sample_once(repo_root, base_path, job_id, vids, source_names))
            if i % max(1, math.ceil(60 / args.sampling_interval_sec)) == 0:
                print(f"Progress: sample {i + 1}/{sample_count}")
            elapsed = time.time() - t0
            time.sleep(max(0.0, args.sampling_interval_sec - elapsed))
    finally:
        shell(f"kubectl delete -f '{manifest}'", repo_root, check=False)
        wait_no_deployment(repo_root, args.cleanup_timeout_sec)

    with samples_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "timestamp_epoch",
                "source_throughput_records_per_sec",
                "total_managed_memory_used_bytes",
                "total_slots_used",
            ]
        )
        writer.writerows(rows)

    commit_out, commit_rc = shell("git rev-parse --short HEAD", repo_root, check=False)
    run_commit = commit_out if commit_rc == 0 and commit_out else "not-captured"
    run_row = build_run_row(args, run_id, run_commit)
    append_run_row(runs_csv, run_row)

    if args.plot:
        plot_cmd = [
            sys.executable,
            str(results_root / "plot_run.py"),
            "--query",
            query,
            "--run-id",
            run_id,
            "--results-root",
            str(results_root),
        ]
        proc = subprocess.run(plot_cmd, cwd=repo_root, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if proc.returncode != 0:
            print("Plot generation failed:")
            print(proc.stdout)
            raise RuntimeError("plot_run.py failed")
        print(proc.stdout.strip())

    print(f"Saved samples: {samples_csv}")
    print(f"Updated runs: {runs_csv}")
    print("Run complete.")


if __name__ == "__main__":
    main()
