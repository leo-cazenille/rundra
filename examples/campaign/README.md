# Two-target campaign example

This example assigns one portable experiment to two already configured
detached targets. Replace `cluster-a` and `cluster-b` with target names from
your `~/.config/rundra/targets.yaml`; do not put backend definitions or
credentials in these project files.

Named project campaign:

```bash
rundr doctor experiment.yaml --campaign two-clusters --connect
rundr plan experiment.yaml --campaign two-clusters
rundr submit experiment.yaml --campaign two-clusters
rundr wait CAMPAIGN_ID --progress
rundr fetch CAMPAIGN_ID --mode copy --extract
```

Equivalent standalone definition:

```bash
rundr plan campaign.yaml
rundr submit campaign.yaml
```

Each launch creates one child Run and writes below its own destination.
Campaign Task selectors use `launch-name/task_NNNNNN`.
