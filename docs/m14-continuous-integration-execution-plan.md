# M14 - Continuous integration gates

## Objective

Make every `main` commit and pull request receive fast, infrastructure-free
quality and packaging checks while retaining scheduler-cluster and live-cluster
tests at appropriate scheduled or explicit opt-in boundaries.

## Implementation

1. Provide one repository command for pytest, Ruff lint and format checks, and
   strict mypy validation.
2. Provide one distribution command for privacy auditing, metadata checking,
   clean Python 3.12 installation, and installed-CLI smoke testing.
3. Run both commands as independent GitHub status checks on `main` pushes and
   pull requests, cancelling superseded checks for the same branch without
   duplicating feature-branch runs.
4. Reuse the commands in release validation so release and commit gates cannot
   silently diverge.
5. Run the Docker OpenPBS lifecycle boundary weekly and on demand, with cached
   scheduler-base layers and retained failure diagnostics.
6. Keep Docker Slurm nightly/manual, Slurm cgroup manual-only, and Shoal tests
   explicitly authorized and manually invoked.
7. Type-check workflow syntax and expression contexts on every commit, expose
   workflow health in the README, and limit monthly GitHub Actions and `uv`
   dependency updates to one grouped pull request per ecosystem without
   automatic merging.

Branch protection remains repository-host configuration. Maintainers should
require the `quality` and `package` checks before merging without making
external scheduler availability a merge prerequisite.
