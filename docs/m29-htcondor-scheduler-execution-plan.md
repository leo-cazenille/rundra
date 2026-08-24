# M29 - HTCondor scheduler backend

## Objective

Add a reliable-core HTCondor backend without weakening Rundra's portable
scheduler boundary. The first release targets a submit/access point connected
to execute nodes through an explicitly shared workspace.

## Scope

1. Replace scheduler-name conditionals with an immutable built-in scheduler
   descriptor registry for capabilities, required tools, adapter construction,
   and resource validation.
2. Add targets schema version 9 with `scheduler.type: htcondor` and mandatory
   `shared_workspace: true` acknowledgement.
3. Submit homogeneous Task sets as vanilla-universe clusters, map every Task to
   a durable `ClusterId.ProcId`, query `condor_q` then `condor_history`, cancel
   exact identities, and expose normalized logs, nodes, timestamps, exits,
   holds, and removals.
4. Own portable CPU, memory, GPU, walltime, output, error, and event-log policy.
   Allow only validated accounting group, requirements, rank, priority, disk,
   and concurrency-limit native fields.
5. Add parser, adapter, registry, lifecycle, and opt-in Docker system coverage,
   plus operator and agent documentation.

## Deliberate exclusions

- HTCondor file transfer and credential delegation;
- DAGMan or scheduler dependencies;
- compact Rundra worker pools and scheduler rerun recovery;
- arbitrary submit-description directives;
- target-side image/application preparation;
- schedd discovery, federation, and remote pool selection.

These capabilities require separate contracts and must not be inferred from
the scheduler name.

## Acceptance

- Existing local, Slurm, and OpenPBS behavior remains unchanged.
- A version-9 HTCondor target is rejected unless shared visibility is explicit.
- Submission receipt parsing proves the exact cluster/process range before the
  Run becomes durably submitted.
- Active and historical jobs produce portable Task states without parsing
  human-formatted scheduler output.
- Unit tests run without HTCondor; the Docker boundary is explicit opt-in.
