# v0.1 interface stability

This document defines the compatibility boundary for the forthcoming Rundra
v0.1 release. The command-line interface and documented serialized formats are
the supported integration surface. Rundra does not expose a supported Python
API in v0.1.

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

## Python imports are internal

All imports below `rundra.*`, including domain dataclasses, ports, adapters,
orchestration services, result objects, and configuration loaders, are internal
in v0.1. Their names, signatures, modules, and composition may change without a
deprecation period. The root package intentionally exports no Python API.

Tests import these modules to exercise implementation boundaries; that does not
make them supported application interfaces. A future Python API will sit above
the orchestration layer, receive its own documentation and compatibility
policy, and will not be inferred from current internal imports.

## Compatibility changes

Every public-format change must update the specification, checked fixture, and
contract test in the same commit. Breaking serialized-format changes require a
new version and explicit migration behavior. Security fixes may reject inputs
that should never have been accepted, such as credential fields, unsafe paths,
or invalid backend-native tokens.
