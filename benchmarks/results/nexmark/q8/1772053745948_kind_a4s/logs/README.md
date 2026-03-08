This folder stores log artifacts for run `1772053745948_kind_a4s`.

Saved files:
- `experiment_driver_terminal.log`: full terminal output from `run_nexmark_experiment.py`
  (includes timestamped throughput/memory/slot samples and run lifecycle messages).
- `jobmanager_a4s_curve_partial.log`
- `taskmanager_a4s_curve_partial.log`
- `operator_a4s_curve_partial.log`

Note:
- The `*_a4s_curve_partial.log` files were created during a late snapshot attempt and are empty.
- The main benchmark script deletes the FlinkDeployment at the end of sampling, so JobManager/TaskManager pod logs are no longer retrievable after cleanup.
