# M26 container-runtime provenance plan

## Goal

Record the actual container-runtime identity selected for a Run without making
pure planning execute commands or changing existing RunRecord document shapes.

## Contract

- `ContainerRuntime` remains backward compatible. An optional runtime identity
  port returns the existing typed `CapabilityCheck` value.
- Native and Apptainer adapters implement the identity port. Apptainer executes
  one shell-free `version` command locally or through the configured transport.
- Version output must contain exactly one nonempty line, contain no NUL, and be
  at most 256 characters. External stderr and command details are not persisted.
- Actual `run` and `submit` orchestration performs the identity query after
  availability checks and before staging. A failed query fails the registered
  Run before scientific work can be submitted.
- Existing scalar metadata stores `container_runtime` and optional
  `container_runtime_version`; v1-v4 readers and exact field sets are unchanged.
- `validate`, `plan`, `doctor`, and remote preflight retain their network-free
  or non-executing runtime-check behavior.

## Acceptance

- Local native Runs persist `container_runtime: native`.
- Local and remote Apptainer adapters return a bounded observed version using
  argument-vector execution without a shell.
- Existing third-party and fake runtimes that only implement `ContainerRuntime`
  remain usable and simply provide no new identity metadata.
