## JUSTIN & A4S Autoscaling Extensions

This repository contains the code for Justin and its A4S-based autoscaling extensions on top of Flink and the Flink Kubernetes Operator. It includes:

- **Justin**: the original autoscaler and experimental setup from the paper.
- **A4S autoscaling**: a cache-aware autoscaler that uses stack distance histograms, miss rate curves (MRCs), and memory–parallelism curves (MPCs) to drive scaling decisions.
- **Benchmark harness**: Nexmark-based workloads, experiment configs (DS2 / Justin / A4S), and scripts for running and plotting experiments.

### Repository layout (high level)

- `flink/` – Forked Flink runtime and operator:
  - A4S autoscaler integration
  - Stack distance histogram collection and aggregation
  - RocksDB memory and native metric extensions
- `benchmarks/` – Nexmark benchmark harness:
  - Result schemas, run metadata, and plot scripts
  - Per-query experiment results and plots
- `notebooks/nexmark/` – Per-query configs for DS2, Justin, and A4S policies.
- `standalone-stackhistogram/` – Small standalone module for:
  - Merging stack distance histograms
  - Computing unscaled and horizontally scaled miss rate curves

### Running Justin & A4S experiments (local Kind cluster)

The simplest way to test Justin and A4S is to deploy a local Kubernetes cluster using Kind.

1. **Install dependencies**
   - See `Requirements.md` for installing:
     - Jupyter Notebook
     - Kind
     - Helm
     - Kubectl
   - This also covers building the Flink and Flink Kubernetes Operator JARs using Docker.

2. **Deploy a local cluster**
   - Follow `Deployment.md` to:
     - Create a Kind cluster
     - Deploy Flink, the Flink Kubernetes Operator, and supporting services (Prometheus, Grafana, …)

3. **Run benchmarks**
   - See `Benchmarks.md` for:
     - Running Justin and A4S on Nexmark workloads
     - Collecting results and generating plots from the `benchmarks/results` directory

### Running on Grid5000

If you have access to Grid5000, you can reproduce the paper’s large-scale experiments.

1. **Install dependencies on Grid5000**
   - `Requirements_g5k.md` explains how to install:
     - Terraform
     - Helm
     - Kubectl
   - It also covers building the Flink and Flink Kubernetes Operator JARs via Docker.

2. **Deploy a cluster**
   - `Deployment_g5k.md` describes:
     - Provisioning a cluster with Terraform
     - Deploying Flink, the operator, and monitoring stack (Prometheus, Grafana, …)

3. **Run benchmarks on Grid5000**
   - `Benchmarks_g5k.md` explains how to:
     - Run the full set of Justin and A4S experiments
     - Collect and interpret benchmark results

For more details on the autoscaling internals (A4S, MRC/MPC, stack histograms, and RocksDB integration), see the sections below and:

- `AGENTS.md` – high-level guidance for working with the codebase using agents.
- `Benchmarks.md` – experiment design and how to run them locally.
- A4S implementation and tests under `flink/flink-kubernetes-operator` and `flink/flink-runtime`.

---

### Detailed change summary

#### A4S autoscaling pipeline (operator & policy)

- **A4S core logic**
  - `A4S`: main entry point for the A4S autoscaling policy.
  - `MemoryParallelismCurve`: builds MPCs from MRCs and supports selecting target parallelism.
  - `MissRateCurve`: builds miss rate curves from stack distance histograms.
  - Policy refinements:
    - MPC generation from MRC with unit tests.
    - Scaling decisions that account for current throughput.
    - Greedy selection of the “best” point along the MPC.
    - Standardized timing (seconds) across policy and tests.
    - A reworked A4S scaling policy (`a4s` files and tests).

- **Operator integration**
  - Extended autoscaler components:
    - `ScalingExecutor`, `ScalingMetricCollector`, `ScalingMetricEvaluator`.
    - `CollectedMetrics`, `EvaluatedMetrics`, `ScalingMetric`.
    - `AutoScalerOptions` with A4S-specific flags (enabling policy, tuning thresholds, etc.).
  - New A4S REST/metrics types (operator side):
    - `A4SAggregatedMetricsResponseBody`, `A4SAggregatedVertexMetricsHeaders`.
  - Operator services & tests:
    - Updated `AbstractFlinkService`, `FlinkService`, `TestingFlinkService`, and `ClusterHealthObserver` to understand A4S metrics.
    - Tests in `AbstractFlinkServiceTest` and `RestApiMetricsCollectorTest` cover the new paths.

#### Stack distance histograms & A4S runtime metrics

- **Metric plumbing in Flink runtime**
  - New histogram-related types:
    - `StackDistanceHistogramProvider`, `StackDistanceHistogramResult`.
  - Integrated into existing metric infrastructure:
    - `MetricNames`, `MetricDump`, `MetricQueryService`, `QueryScopeInfo`.
    - `MetricQueryServiceGateway`, `TestingMetricQueryServiceGateway`.
    - `MetricFetcherImpl`, `MetricStore`.
  - New A4S aggregation REST handler (runtime side):
    - `A4SAggregatingVertexMetricsHandler` plus runtime `A4SAggregatedMetricsResponseBody` and `A4SAggregatedVertexMetricsHeaders`.

- **Standalone / runtime stack histogram + MRC implementation**
  - `standalone-stackhistogram/StackHistogram.java`:
    - Bucketed stack distance histogram with configurable number of buckets \(H\) and max distance \(D_\mathrm{max}\).
    - Supports merging multiple histograms by summing counts per bucket and aggregating `numPartitions`.
  - `standalone-stackhistogram/QuickMRC.java`:
    - **Unscaled MRC**: for each cache size \(S\), computes `miss_rate(S) = 1 − hits(S)/total`, where hits are all buckets fully below \(S\).
    - **Scaled MRC**: horizontally scales the cache-size axis (e.g. multiply by number of tasks); miss rate is unchanged.
  - `standalone-stackhistogram/QuickMRCTest.java`:
    - Tests random histogram merging (bucket sums, partition counts).
    - Tests unscaled MRC math (S=0 ⇒ miss≈1, large S ⇒ miss≈0).
    - Tests horizontal scaling (cache size × factor; miss rate invariant).
    - End-to-end tests: merge → unscaled MRC → scaled MRC.

#### RocksDB memory management & native metrics

- **Memory configuration and sharing**
  - `RocksDBMemoryConfiguration`, `RocksDBMemoryControllerUtils`:
    - Compute and apply shard-aware, taskmanager-wide memory budgets for RocksDB.
  - `RocksDBOptions`, `RocksDBResourceContainer`, `RocksDBSharedResourcesFactory`:
    - Support configurable number of LRU cache shards and shared caches across operators/tasks.
  - Restore operations updated to respect the new memory configuration:
    - `RocksDBFullRestoreOperation`, `RocksDBHeapTimersFullRestoreOperation`,
      `RocksDBIncrementalRestoreOperation`, `RocksDBNoneRestoreOperation`, `RocksDBHandle`.
  - Tests:
    - `RocksDBMemoryControllerUtilsTest`, `RocksDBStateBackendConfigTest`,
      `TaskManagerWideRocksDbMemorySharingITCase`.

- **RocksDB native metrics / stack histograms**
  - `RocksDBNativeMetricMonitor`, `RocksDBNativeMetricOptions`:
    - Expose stack distance histograms and additional RocksDB internals as Flink metrics.
  - Custom RocksDB build & JNI integration:
    - `custom-libs/rocksdbjni-6.20.3-linux64.jar`.
    - JNI calls to retrieve stack distance histograms from RocksDB.
  - `flink-statebackend-rocksdb/pom.xml` and related classes updated to use the custom build and new options.

#### Nexmark benchmarks & experiment harness

- **Result schema & tools**
  - Standard CSV schemas:
    - `benchmarks/results/nexmark/runs.template.csv` – per-run metadata.
    - `benchmarks/results/nexmark/samples.template.csv` – per-sample time series.
  - Python scripts:
    - `benchmarks/results/plot_run.py` – generate plots from `samples.csv`.
    - `benchmarks/results/run_nexmark_experiment.py` – run experiments and populate `runs.csv` and samples/plots.

- **Per-query configurations**
  - For each Nexmark query \(q1, q2, q3, q5, q8, q11, q20\):
    - `query*.ds2.yaml` – DS2 baseline (original configs renamed from `query*.yaml`).
    - `query*.justin.yaml` – Justin policy configs.
    - `query*.a4s.yaml` – A4S policy configs.

- **Recorded results**
  - Under `benchmarks/results/nexmark/q*/...`:
    - `runs.csv` – list of all runs and their metadata.
    - Per-run directories `<timestamp>_kind_<policy>/` with:
      - `samples.csv` – time-series metrics.
      - `plot.png` – rendered performance plots.
  - Per-query readmes:
    - `benchmarks/results/nexmark/q{1,2,3,5,8,11}/README.md` describe setup and findings.

#### Operator, CRDs, and documentation

- **CRD extensions**
  - `flinkdeployments.flink.apache.org-v1.yml` and related CRDs:
    - New fields for enabling A4S, tuning its parameters, and exposing additional metrics.
    - Updated schemas for session jobs and state snapshots where relevant.

- **REST & WebMonitor updates**
  - `WebMonitorEndpoint` and legacy metric handlers:
    - Register A4S aggregated metrics endpoints.
    - Ensure metrics are reachable from the Web UI and operator metric collection.

- **Miscellaneous project docs & artifacts**
  - `AGENTS.md` – instructions and conventions for using AI/agents with this repo.
  - `Benchmarks.md`, `Benchmarks_g5k.md` – detailed instructions for local and Grid5000 runs.
  - `.gitignore` – updated to ignore new artifacts (JARs, PDFs, logs, plots).
  - Reference materials:
    - `a4s.pdf`, `justin.pdf` – papers or design docs.
    - `nexmark-flink-0.3-SNAPSHOT.jar` – benchmark JAR.
