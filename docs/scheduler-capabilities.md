# Scheduler capabilities

Rundra derives scheduler capabilities from the installed adapter. Target files
select `local`, `slurm`, or `pbs`; they cannot add or override capabilities.

| Capability | local | Slurm | OpenPBS |
| --- | --- | --- | --- |
| detached submission | no | yes | yes |
| arrays | no | yes | yes |
| dependencies | no | yes | yes |
| compact worker pool | no | yes | yes |
| scheduler rerun recovery | no | yes | no |
| scheduler probe | no | yes | yes |

Inspect this metadata through `rundr targets --json`, `rundr doctor --json`, or
plan v8 JSON. Agents must use these fields instead of inferring behavior from a
scheduler name.

OpenPBS compact workers support preparation dependencies, concurrent lanes,
compact Task state, failure propagation, cancellation, and retrieval. OpenPBS
targets that select worker-pool execution must set `requeue_limit: 0` because
scheduler-driven reruns are not supported. Framework-level Task retry limits
remain independent.

For a remote offline audit, use `rundr doctor EXPERIMENT --offline --connect
--prepare-location auto|local|target --json`. The command verifies immutable
cache inputs without Git fetches, image pulls, builds, compilation, or scheduler
submission. A target cache is never assumed warm without a connection probe.
