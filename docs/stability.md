# v0.1 interface stability

This document defines the compatibility boundary for the published Rundra 0.1
release line. The command-line interface and documented serialized formats are
the primary supported integration surface. The result readers documented below
are the only supported Python API in the 0.1 line.

## Frozen v0.1 surfaces

The following interfaces are frozen for v0.1:

- the `rundr` command names, option names, positional arguments, selection
  semantics, launch precedence, default locations, and exit meanings described
  in the [CLI reference](cli-reference.md);
- experiment, target, project-launch, and user-launch YAML with
  `version: 1`;
- the `format_version: 1` CLI documents and RunRecord represented by the
  checked [contract fixtures](schemas/README.md);
- portable execution/retrieval state strings and stable Run/Task identifiers.

The checked [`cli-surface-v1.json`](schemas/cli-surface-v1.json) fixture prevents
command or option removal/renaming from happening silently. The operation
fixtures test JSON fields and values, while the complete RunRecord fixture tests
durable serialization.

Within format version 1, consumers must tolerate documented additive CLI JSON
fields. Removing a documented field, changing its type or meaning, or
reinterpreting a YAML field requires a new format/schema version. Persisted
RunRecords are intentionally stricter: unknown fields and unsupported versions
are rejected. A new Rundra release may add commands or options, but it must not
silently change the meaning of a frozen v0.1 invocation.

Human-readable prose, whitespace, and layout are not stable. The physical
RunStore directory layout and remote workspace layout are implementation
details; use CLI operations and Run IDs rather than discovering files.

## Python imports

`rundra.artifacts.open_result_set` and `open_result_shard`, together with the
objects they return for documented read operations, are the narrow supported
Python result-reading API. Their behavior is described in
[artifact and provenance semantics](artifacts-and-provenance.md).

All other imports below `rundra.*`, including domain dataclasses, ports,
adapters, orchestration services, CLI result objects, and configuration loaders,
are internal. Tests importing them do not make them supported application
interfaces.

## Compatibility changes

Every public-format change must update the specification, checked fixture, and
contract test in the same commit. Breaking serialized-format changes require a
new version and explicit migration behavior. Security fixes may reject inputs
that should never have been accepted, such as credential fields, unsafe paths,
or invalid backend-native tokens.
