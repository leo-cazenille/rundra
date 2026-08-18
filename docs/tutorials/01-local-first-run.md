# Tutorial: first local Run

```bash
uv run rundr doctor examples/minimal/experiment.yaml
uv run rundr plan examples/minimal/experiment.yaml --seed 17
uv run rundr run examples/minimal/experiment.yaml --seed 17 --progress
```

The plan is offline. Record the Run ID printed by `run`; it is the stable handle
for status, logs, inspection, retrieval, cancellation, and cleanup.
