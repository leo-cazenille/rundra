# M14 explicit worker scaling and launch consistency

## Objective

Make scalable `plan`, `run`, and `submit` choose the same worker-pool shape and
resource allocation. Separate target-owned safety ceilings from conservative
target defaults and explicit per-Run scale requests, so Rundra never consumes
all permitted nodes merely because a target permits that scale.

## Compatibility

- Retain exact targets versions 1 through 5 parsing and behavior.
- Add targets version 6 for default/maximum worker counts and slots.
- Keep portable experiment schema version 1 and project schema version 2.
- Keep existing plan and RunRecord JSON versions; add fields only to the current
  scalable plan rendering and scheduler metadata contracts.
- Continue treating one worker as one single-node allocation. Physical node
  placement remains scheduler-controlled and is never inferred offline.

## Increments

1. Add a typed execution-scale request and target-v6 worker-pool fields:
   `default_workers`, `max_workers`, `default_task_slots_per_worker`, and
   `max_task_slots_per_worker`. Validate defaults against maxima and both
   against `max_active_tasks` and `max_concurrent_jobs`.
2. Add `--workers` and `--task-slots-per-worker` to `plan`, `run`, and
   `submit`. Allow project-v2 defaults and profiles to provide the same values;
   resolve them with CLI > project profile/default > target default precedence.
3. Factor one scaling-decision path used by planning and execution. Keep
   execution task materialization unchanged, but pass the chosen worker count,
   slots, and worker resources into scheduler submission without recomputing
   them from mutable target configuration.
4. Require the Slurm adapter to allocate the exact effective worker resources.
   Persist requested/effective scale and render policy ceilings, worker
   allocation resources, concurrent capacity, and scheduler-controlled node
   placement clearly.
5. Update the Shoal target to permit 8 workers by 40 slots while defaulting to
   one 40-slot node. Add an explicit full-cluster project profile and reduce the
   Pogosim memory request to 1 GiB, consistent with measured ~210 MiB peak RSS
   plus a conservative margin.
6. Add parser, precedence, policy rejection, plan/submit consistency, Slurm
   directive, rendering, and lifecycle regression tests. Run the complete unit
   suite, Ruff checks, mypy, and the minimal local example where applicable.

## Safety semantics

- Target maxima are hard policy. Exceeding one is an actionable error, never a
  silent clamp.
- Defaults choose requested scale; they do not describe available topology.
- Generic target-v6 defaults may remain one worker and one slot. Site owners
  explicitly opt into larger defaults.
- Full-cluster use requires an explicit CLI request or named project profile.
- Worker memory is the sum of logical per-Task memory requests. Rundra does not
  overcommit memory or infer application usage.
