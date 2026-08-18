# Tutorial: large sweeps and analysis

Review Task count, worker count, slots, concurrent capacity, lane depth,
aggregate memory, sharding, and confirmation threshold in `plan`.

```bash
rundr submit experiment.yaml --seeds 0:9999 --confirm-tasks 20000
rundr wait RUN_ID
rundr fetch RUN_ID --destination retrieved/sweep
```

Run application-owned analysis on an approved workstation or scheduled compute
node, writing derived products outside the raw retrieval tree.
