# Running benchmarks

## Motivation benchmarks
Open the [./notebooks/motivation/xp.ipynb](http://localhost:8888/notebooks/notebooks/motivation/xp.ipynb) notebook in your browser (make sure your jupyter server is still up and running, following the Requirements.md instruction).

Before continuing, make sure that the Flink image name in the following file is the same as the one you used during the build phase:
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink
spec:
  image: flink-justin:dais <------- Your Flink image name
```
1. [./notebooks/motivation/read-only/query.yaml](./notebooks/motivation/read-only/query.yaml)
1. [./notebooks/motivation/write-only/query.yaml](./notebooks/motivation/write-only/query.yaml)
1. [./notebooks/motivation/update/query.yaml](./notebooks/motivation/update/query.yaml)

## Nexmark benchmarks
Open the [./notebooks/nexmark/xp.ipynb](http://localhost:8888/notebooks/notebooks/nexmark/xp.ipynb) notebook in your browser (make sure your jupyter server is still up and running, following the Requirements.md instruction).

Recorded run results for long-term comparison are stored under:
- `benchmarks/results/nexmark/`

### Scripted single-query run (async friendly)

You can run one query end-to-end (deploy, sample, cleanup, save, plot) with:

```bash
venv/bin/python benchmarks/results/run_nexmark_experiment.py \
  --query q1 \
  --sampling-interval-sec 5 \
  --duration-sec 600 \
  --policy a4s-justin \
  --note manual
```

Run it asynchronously (example):

```bash
nohup venv/bin/python benchmarks/results/run_nexmark_experiment.py \
  --query q2 \
  --sampling-interval-sec 5 \
  --duration-sec 600 \
  --policy a4s-justin \
  --note overnight > /tmp/q2-benchmark.log 2>&1 &
```

The script automatically:
1. applies the query manifest (`notebooks/nexmark/<query>/query<num>.yaml` by default)
2. samples canonical metrics from Flink REST
3. deletes the deployment at the end
4. writes `<run_id>-samples.csv` and appends `runs.csv`
5. generates `<run_id>-plot.png`

Useful flags:
- `--manifest <path>` to override manifest path
- `--steady-window-sec <sec>` to control steady-state summary window
- `--plot/--no-plot` to enable or disable plot generation
- `--query-name <name>` to override the `query` field in `runs.csv`

Before continuing, make sure that the Flink image name in the following file is the same as the one you used during the build phase:
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink
spec:
  image: flink-justin:dais <------- Your Flink image name
```
1. [./notebooks/motivation/read-only/query.yaml](./notebooks/nexmark/queryX/queryX.yaml), with X being the query number.

The notebook contains a cell for each query. These cells will execute the query twice, one with the default auto scaler, and one with the Justin auto scaler.

### Playing with Justin

As explained in the paper, Justin relies on multiple parameters such as a minimum cache hit rate and minimum state access latency.
These parameters can be modified via the YAML file of the query, namely the lines:
1. `job.autoscaler.cache-hit-rate.min.threshold: "0.8"` (ratio)
2. `job.autoscaler.state-latency.threshold: "1000000.0"` (nano seconds)

Note that changing those values can result in different scaling decisions.

Additionally, the reader can also modify the stabilization interval (`job.autoscaler.stabilization.interval`) and metric window (`job.autoscaler.metrics.window`).

### Modifying the Justin Policy.

The Justin policy is implemented in the Flink Kubernetes Operator's auto scaler module.
The reader can easily apply its own policy logic by modifying the [policy](https://github.com/CloudLargeScale-UCLouvain/flink-justin/blob/a3d6539d40668a92f910ea22adb15e7120884e8c/flink-kubernetes-operator/flink-autoscaler/src/main/java/org/apache/flink/autoscaler/ScalingExecutor.java#L567) method of the [ScalingExecutor.java](./flink-kubernetes-operator/flink-autoscaler/src/main/java/org/apache/flink/autoscaler/ScalingExecutor.java) file. 
The reviewer has access to previous Scaling Decisions (if any) through the `scaling` parameter.

Once modified, the image needs to be rebuilt and pushed to the nodes.
Delete the operator and its resources by executing the script `delete.sh` located in the `scripts`folder. This script will delete any Flink job pending, the operator, and the 3 custom resource definitions.
Once deleted, you can re-deploy the operator following the previous instructions.