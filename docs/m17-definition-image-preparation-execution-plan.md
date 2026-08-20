# M17 - Definition Image Preparation

## Goal

Build immutable Apptainer/Singularity SIF images automatically from trusted
definition files while preserving Rundra's content-addressed caches, scheduler
boundaries, provenance, and prebuilt-image compatibility.

## Public contracts

- Project version 3 supports explicit prebuilt-image recipes and definition
  recipes. It also supports working-tree-only source acquisition.
- Target version 8 owns definition-build enablement, allowed local/target
  locations, unprivileged or fakeroot mode, and bounded build-resource ceilings.
- `--rebuild-image` bypasses only the definition-image recipe cache. Existing
  `--rebuild` continues to bypass only compiled application outputs.
- Auto mode builds cold definition images locally and transfers their verified
  content to remote targets. Forced target mode waits for a bounded preparation
  job to publish the measured digest before submitting scientific work.
- Offline definition preparation is cache-hit-only.

## Image lifecycle

The image recipe key covers the immutable source snapshot, definition path and
content, canonical recipe, target identity, target-owned build mode, builder
platform, and Apptainer/Singularity version. Builds run in isolated temporary
directories and publish the measured SIF as `images/<sha256>.sif`. A separate
atomic recipe index maps the recipe key to that digest and preserves build
metadata and logs. Cache entries are locked, verified, read-only, and never
trusted through names or symlinks.

Definition files are trusted executable project input. Rundra records their
measured output but does not claim that a cold rebuild is reproducible when the
definition references mutable or undeclared external inputs.

## Failure and recovery

No build runs as host root or through sudo. Fakeroot is passed only when target
policy selects it. Local build failure prevents Run submission. Target build
failure marks the registered Run failed and prevents scientific submission.
Interrupted target preparation persists its scheduler identity; resume adopts
the completed image metadata and continues scientific submission exactly once.
Cancellation covers preparation and scientific jobs.

## Delivery

1. Add strict project-v3, target-v8, domain, plan, JSON, and CLI contracts.
2. Implement local definition builds, content/index caches, transfer, rebuild,
   offline behavior, provenance, and failure logs.
3. Implement bounded target builds, wait-before-submit, cancellation, and
   interrupted-submit recovery.
4. Migrate the Python multiprocessing Shoal example to a checked definition
   file and remove its externally supplied SIF requirement.
5. Add fake local/SSH/Slurm tests, Docker coverage, gated cold/warm Shoal tests,
   documentation, and complete quality/distribution verification.
