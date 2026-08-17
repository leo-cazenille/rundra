# M7 execution plan: automated preparation

M7 extends project configuration, planning, execution, and provenance without
changing the portable experiment schema. Implementation proceeds in working
increments so version-1 projects remain unchanged throughout.

## 1. Schemas and pure planning

- Add strict project configuration version 2 and typed preparation models.
- Validate pinned Git commits, pinned SIF SHA-256 values, safe relative paths,
  shell-free build argv, declared outputs, and bounded build resources.
- Discover adjacent project configuration from `validate` and `plan`.
- Emit version-2 plan output that describes identities, possible actions,
  cache scope, requested preparation location, and safety effects without I/O.
- Retain the exact version-1 plan and RunRecord shapes for version-1 projects.

## 2. Content-addressed local preparation

- Snapshot either a pinned Git commit or an explicitly selected working tree.
- Resolve SIF images by verified SHA-256, publishing immutable cache entries
  under per-key locks and atomic renames.
- Build in a writable copy of the source snapshot inside the verified image.
- Verify declared outputs and publish a prepared-source cache using a key made
  from source, image, recipe, cache scope, and platform identity.
- Feed the prepared source and absolute image into the existing staging and
  execution lifecycle.

## 3. Scheduled target preparation

- Probe configured target candidates and content caches without broad scans.
- Submit one bounded Slurm preparation job on a miss; never compile on SSH
  login processes.
- Recheck caches under lock in the job, verify the image, build, verify outputs,
  publish atomically, and preserve preparation logs.
- Submit experiment work with a framework-owned `afterok` dependency.

## 4. Lifecycle and provenance

- Persist version-2 RunRecords containing source/image/build identities,
  actions, hashes, preparation logs, and the separate preparation scheduler ID.
- Finalize remote image and compiled-output cache actions from an atomic
  preparation manifest after synchronous execution.
- Extend status, inspect, logs, fetch, and cancel for preparation state.
- Preserve partial-output retrieval and meaningful nonzero exits.

## 5. Migration and validation

- Migrate Pogosim to an adjacent checked project-v2 recipe.
- The checked Pogosim recipe pins the upstream commit and SIF digest and makes
  `rundr run examples/pogosim-shoal/experiment.yaml --seeds 0:2` the happy path.
- Add unit, fake-adapter, local integration, JSON contract, and gated Shoal
  coverage for cold and warm preparation.
- Run pytest, Ruff lint/format checks, mypy, JSON contracts, and the minimal
  local reproducibility example before completion.
