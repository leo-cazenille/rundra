# Scheduler capabilities

Rundra derives scheduler capabilities from the installed adapter. Target files
select `local`, `slurm`, `pbs`, or `htcondor`; they cannot add or override
capabilities.

| Capability | local | Slurm | OpenPBS | HTCondor |
| --- | --- | --- | --- | --- |
| detached submission | no | yes | yes | yes |
| arrays | no | yes | yes | yes |
| dependencies | no | yes | yes | no |
| compact worker pool | no | yes | yes | no |
| scheduler rerun recovery | no | yes | no | no |
| scheduler probe | no | yes | yes | yes |

Inspect this metadata through `rundr targets --json`, `rundr doctor --json`, or
plan v8 JSON. Agents must use these fields instead of inferring behavior from a
scheduler name.

OpenPBS compact workers support preparation dependencies, concurrent lanes,
compact Task state, failure propagation, cancellation, and retrieval. OpenPBS
targets that select worker-pool execution must set `requeue_limit: 0` because
scheduler-driven reruns are not supported. Framework-level Task retry limits
remain independent.

HTCondor v0.1 support uses vanilla jobs with `should_transfer_files = NO` and
therefore requires a workspace visible at the same absolute path from the
access point and every execute node. Target schema version 9 requires the site
operator to acknowledge this with `shared_workspace: true`. Rundra maps Tasks
to `ClusterId.ProcId`, checks active jobs with `condor_q`, reconciles completed
jobs through `condor_history`, and owns output/error/event-log paths. It does
not yet provide dependencies, target-side preparation, compact worker pools,
file transfer, DAGMan, or scheduler rerun recovery.

The accepted `resources.native.htcondor` fields are `accounting_group`,
`requirements`, `rank`, `job_priority`, `request_disk`, and
`concurrency_limits`. They are validated as typed values or safe single-line
ClassAd expressions. Arbitrary submit directives are rejected.

For a remote offline audit, use `rundr doctor EXPERIMENT --offline --connect
--prepare-location auto|local|target --json`. The command verifies immutable
cache inputs without Git fetches, image pulls, builds, compilation, or scheduler
submission. A target cache is never assumed warm without a connection probe.
