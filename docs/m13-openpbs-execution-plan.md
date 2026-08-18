# M13 - OpenPBS scheduler backend

## Objective

Add a production-capable `scheduler.type: pbs` backend, tested against OpenPBS,
without changing existing Slurm documents or leaking PBS concepts into portable
experiment models. This is targeted for v0.2.0, not v0.1.1.

## Implementation

1. Replace scheduler-kind branches in planning and CLI wiring with capability
   selection and scheduler/preflight factories.
2. Add typed PBS rendering and adapters for qsub, JSON qstat, qdel, arrays,
   after-success dependencies, logs, opaque server-qualified IDs, and portable
   state/exit mapping.
3. Add strict `resources.native.pbs` policy while retaining portable ownership
   of CPU, GPU, memory, walltime, node, and task requests.
4. Generalize array strategy and persisted mapping validation through additive
   plan and RunRecord schema versions; all existing readers remain supported.
5. Reuse the container-cluster harness for OpenPBS and require parity for
   preparation, single jobs, arrays, worker pools, failures, cancellation,
   status, logs, retrieval, and compact large-Task records.
