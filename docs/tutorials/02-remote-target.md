# Tutorial: remote target setup

Store site details in `~/.config/rundra/targets.yaml`, not project configuration.

```bash
rundr doctor experiment.yaml
rundr doctor experiment.yaml --connect --json
rundr plan experiment.yaml --seeds 0:9
```

Review aggregate worker CPU and memory as well as per-Task resources. Never run
scientific work on the configured login/controller host.
