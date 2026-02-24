# Flink Justin Agent Guide

## Purpose
This repository contains:
- Flink Runtime `1.18` with custom modifications
- Flink Kubernetes Operator `1.13` with custom modifications

This codebase is the open-source result of the Justin autoscaler work in the Kubernetes operator (see `justin.pdf`), and is used to extend that work with A4S autoscaling ideas (see `a4s.pdf`).

## Environment
- Primary local test target: `kind`
- Secondary test target: 3 worker nodes (`c153`, `c157`, `c165`) + control plane (`c159`)
- Remote access example: `ssh -J ryanl123@fs.csl.utoronto.ca ryanl123@c159`
- Host OS baseline: Ubuntu `24.04.3 LTS`
- Uses Docker daemon as the CNI runtime

Infrastructure setup scripts:
- Common: `scripts/infra/common/common_modules.sh`
- Kind-specific: `scripts/infra/common/kind/`

Runtime/tooling assumptions:
- Java 11 for both Flink Runtime and Flink Kubernetes Operator
- `helm`, `kubectl`, and related Kubernetes tooling already installed

## Build and Images
- Always load the current context with `--load .` when building images
- Build the operator image from `flink-kubernetes-operator/` so `docker-entrypoint.sh` is in build context (do not build that image from repo root).

## Unit Tests (Maven)
Run tests from the correct project root:

- Flink runtime tree (`flink/`):
  - `cd flink && ./mvnw test`
  - Module-only: `cd flink && ./mvnw -pl <module-artifact-id> -am test`
  - Single test: `cd flink && ./mvnw -Dtest=<TestClass>[#testMethod] test`

- Flink Kubernetes Operator tree (`flink-kubernetes-operator/`):
  - `cd flink-kubernetes-operator && mvn test`
  - Module-only: `cd flink-kubernetes-operator && mvn -pl <module-artifact-id> -am test`
  - Single test: `cd flink-kubernetes-operator && mvn -Dtest=<TestClass>[#testMethod] test`

### Flink Kubernetes Operator
- Build from `flink-kubernetes-operator/Dockerfile`
- Required image tag: `flink-kubernetes-operator:dais`

Install from repo root:
```bash
helm install flink-kubernetes-operator ./flink-kubernetes-operator/helm/flink-kubernetes-operator --set image.repository=flink-kubernetes-operator --set image.tag=dais -f ./flink-kubernetes-operator/examples/autoscaling/values.yaml
```

### Flink Runtime
- Build from the root `Dockerfile`
- Build time is long (~15 minutes)
- Required image tag: `flink-justin:dais`
- Includes a custom RocksDB fork under `custom-libs/`

Load image into `kind`:
```bash
kind load docker-image <image_name>
```

## Deploy and Run Experiments
Use Nexmark queries to evaluate runtime behavior.

Operational rule for all `kubectl` commands:
- Verify cluster state conservatively after each change before issuing the next command
- Before applying `FlinkDeployment` manifests, verify `flinkdeployments.flink.apache.org` CRD exists and the matching operator release is running.

Example:

```bash
kubectl apply -f notebooks/nexmark/q8/query8.yaml
```

Clean up after each run:

```bash
kubectl delete -f notebooks/nexmark/q8/query8.yaml
```

## Evaluation Metrics
Applying a query sends metrics to Prometheus and triggers TaskManager scaling. Track at least:
1. Source throughput: `sum(flink_taskmanager_job_task_operator_numRecordsOutPerSecond{operator_name=~"Source.*"})`
2. Total managed memory used: `flink_taskmanager_Status_Flink_Memory_Managed_Used`
3. Total task slots used across TaskManagers

Metric guidelines:
- Query metric once every 5 seconds
- Query metrics as soon as the flink runtime is available
- Standardize all benchmarks to run over a 10 minute window

## Benchmark Result Saving Conventions
Use a consistent, append-only layout under `benchmarks/results/` so runs are comparable and reproducible.

Directory layout (Nexmark):
- Per-query folder: `benchmarks/results/nexmark/<query>/` (example: `q8`)
- Required files per query folder:
  - `runs.csv`: one row per run summary (append only; do not rewrite prior rows)
  - `<run_id>-samples.csv`: raw time-series samples for that run
  - Optional run notes in `README.md`
- Header templates:
  - `benchmarks/results/nexmark/runs.template.csv`
  - `benchmarks/results/nexmark/samples.template.csv`

Run ID convention:
- `<YYYY-MM-DD>-<env>-<query>-<policy>-<note>`
- Example: `2026-02-24-kind-q8-a4s-justin-baseline`
- Keep `run_id` identical across `runs.csv`, samples filename, and generated plot filename.

Required `runs.csv` fields (minimum):
- `run_id,iso_date,environment,run_commit,autoscaler`
- `iso_date` must be full ISO-8601 (example: `2026-02-24T00:00:00Z`).
- `run_commit` should be the latest git commit hash used for that run; use `not-captured` only if unavailable.
- `autoscaler` should be either `justin` or `a4s`.

Required `<run_id>-samples.csv` fields (minimum):
- `timestamp_epoch`
- `source_throughput_records_per_sec`
- `total_managed_memory_used_bytes`
- `total_slots_used`
- Use monotonic timestamps and a fixed sampling interval for the run.

Units and normalization:
- Throughput in records/sec.
- Managed memory stored in bytes in raw samples (plotting may convert to MiB).
- Slots used stored as numeric count.

Plot output convention:
- Generate one image per run at `<query_dir>/<run_id>-plot.png`.
- Keep y-axis ranges consistent across runs of the same query for visual comparability.

Use Prometheus/Grafana queries to evaluate whether changes improve behavior.
- If Prometheus has no Flink scrape targets, collect equivalent results from Flink REST (`/overview`, `/jobs/<jid>`, `/taskmanagers/*/metrics`) and report that fallback explicitly.

Grafana credentials:
- Username: `admin`
- Password: `prom-operator`

## Git and Coding Guardrails
- Prefer small, atomic commits with descriptive messages
- Never run destructive git commands (for example, `git reset --hard` or `git restore`) unless explicitly asked
- No strict linting rules are enforced; follow existing style in nearby code

## A4S Baseline
- Base commit of this fork: `032c7563`
- All commits above `032c7563` are changes made for A4S integration and experiments

## A4S Architecture (Current)

Relevant recent commits (A4S-focused):
- `c1e4c5fe`: initial A4S files and autoscaler wiring
- `c8a07dd5`: MRC -> MPC generation plus tests
- `a20f0f8c`: A4S policy revamp and evaluator/scaling updates
- `e83d69df`: stack-distance histogram aggregation in JobManager metric fetch path
- `fec64873`: RocksDB JNI histogram retrieval plumbing
- `7478279f`: custom RocksDB build and runtime packaging updates
- `51d1116e`, `006fe937`, `3adf0bb7`: MPC refinement and scaling logic cleanup

### 1) Metrics origination in runtime/state backend
- Task-side state backend exports RocksDB and cache-related signals
- Runtime metrics path supports stack-distance histogram transport via:
  - `StackDistanceHistogramProvider`
  - `StackDistanceHistogramResult`
  - `MetricQueryService#queryStackDistanceHistograms(...)`
- `MetricFetcherImpl` fetches regular metrics and stack-distance histogram payloads, then aggregates histogram buckets at JobManager side before storing metric snapshots

### 2) Autoscaler metric ingestion and evaluation
- Autoscaler collector/evaluator ingests Flink metrics and maps them into `ScalingMetric` values
- RocksDB cache metrics (`ROCKS_DB_BLOCK_CACHE_HIT`, `ROCKS_DB_BLOCK_CACHE_MISS`, derived hit-rate) are evaluated and propagated into `EvaluatedMetrics`
- `ScalingConfigurations` materializes per-vertex `ScalingInformation` including:
  - proposed parallelism
  - average cache hit rate
  - average state access latency
  - average throughput

### 3) Decision pipeline in `ScalingExecutor`
- Base autoscaler computes per-vertex parallelism deltas and scaling summaries
- If `JUSTIN_ENABLED` is set, Justin/A4S post-processing runs on top of those summaries
- If `A4S_ENABLED` is set:
  - `A4S` builds/uses per-operator Memory-Parallelism Curves (MPCs)
  - makes decisions as `(parallelism, memoryMB)`
  - converts memory to Justin-compatible discrete memory levels
  - writes overrides back through existing autoscaler state store
- If `A4S_ENABLED` is not set, Justin policy path is used

### 4) MRC/MPC model path
- Stack-distance data feeds MRC estimation (`QuickMRC`, A4S curve classes)
- MRC is transformed into MPC (memory required for target/operator parallelism regions)
- A4S placement logic attempts feasible memory/parallelism points for all operators; if infeasible, it incrementally increases selected operator parallelism and retries

### 5) Realization boundary (important for A4S)
- A4S and Justin outputs are still expressed via existing autoscaler interfaces:
  - parallelism overrides per vertex
  - resource profile (memory level) overrides
- Cluster placement and scheduling remain the responsibility of existing realization/orchestration layers (Flink and Kubernetes operator path), not A4S policy code

## Why this matters for A4S-on-Justin
- A4S is implemented as a policy/model extension, not a replacement for Justin's end-to-end control loop
- As long as A4S keeps producing decisions through existing Justin override interfaces, operator placement details stay decoupled and handled by runtime/orchestrator machinery

# AGENTS Guidance

## Continuous Improvement Rule
- Whenever the agent fixes an issue, or is called out for a mistake, it should immediately update this guidance (or `AGENT.md`) with a concise, reusable rule that would have prevented the issue.
- Keep each added rule specific, actionable, and short.
- Do not add duplicate rules; prefer refining an existing related rule.
