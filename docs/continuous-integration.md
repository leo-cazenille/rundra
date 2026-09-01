# Continuous integration

Rundra separates deterministic commit gates from infrastructure system tests.
The `CI` workflow runs on every pull request and every push to `main` with
read-only repository permissions. Superseded runs for the same branch are
cancelled. Feature-branch pushes begin validation when their pull request is
opened, avoiding duplicate push and pull-request checks for the same commit.

## Required commit checks

The `quality` job runs the same command maintainers use locally:

```bash
tools/check.sh
```

It executes ordinary pytest, Ruff lint and formatting checks, and strict mypy
validation. Pytest's Docker and Shoal system tests remain skipped unless their
explicit command-line opt-ins are present. Before Python validation, actionlint
checks every workflow's syntax, expression contexts, job dependencies, cron
syntax, and embedded shell scripts. This catches invalid workflows even when
the invalid file cannot start its own GitHub Actions run.

The `package` job builds the wheel and source distribution and then runs:

```bash
tools/check_distribution.sh dist/*.whl dist/*.tar.gz
```

This audits the publication boundary, checks package metadata, installs the
wheel and its dependencies into a clean Python 3.12 virtual environment, and
smoke-tests `rundr --version` and `rundr help`.

Configure the default branch's GitHub protection rule to require `quality` and
`package`. Do not require scheduled scheduler-system jobs: an external Docker
or registry outage must not prevent an otherwise valid merge.

## Local deployment workflow

The `local-deployment` workflow runs on every push to `main`, every pull
request, and manual dispatch. It builds the wheel, installs that wheel and its
runtime dependencies into a clean Python 3.12 environment, verifies the
installed `rundr` console script, and invokes the 40-Task local worker-pool
integration test through that installed executable. This catches packaging,
entry-point, local staging, bounded concurrency, Run persistence, and retrieval
regressions without Docker or cluster credentials.

## Scheduler-system workflows

| Workflow | Automatic trigger | Manual trigger | Purpose |
| --- | --- | --- | --- |
| `docker-slurm-system` | nightly at 02:23 UTC | yes | Slurm lifecycle, scale, failure, cancellation, and retrieval |
| `docker-pbs-system` | Wednesdays at 03:41 UTC | yes | OpenPBS arrays, failure, cancellation, and retrieval |
| `docker-htcondor-system` | Thursdays at 04:17 UTC | yes | HTCondor submission, lifecycle, cancellation, and retrieval |
| `docker-campaign-system` | Tuesdays at 03:07 UTC | yes | Two-target campaign planning, concurrent child Runs, aggregate lifecycle, and retrieval |
| `Docker Slurm cgroup system` | none | yes | privileged cgroup-v2 memory enforcement |
| Shoal system tests | none | local explicit opt-ins only | live reference-cluster acceptance evidence |

The campaign workflow reuses the Docker Slurm image but selects a focused
campaign suite with two independently named detached targets. It requires both
child Runs to be observed running concurrently before aggregate wait and fetch.
The OpenPBS workflow caches the shared Slurm/Apptainer base-image layers. The
OpenPBS layer itself is built from the pinned source in the checked Dockerfile.
The Slurm and OpenPBS lifecycle workflows upload a 14-day diagnostic artifact
after a failure.

## Dependency updates

Dependabot checks pinned GitHub Actions monthly and checks Python dependencies
managed by `uv` shortly afterward. Each ecosystem produces one grouped update
with at most one open pull request, limiting routine dependency maintenance to
two PRs per month. Dependabot does not merge changes: every proposed upgrade
must pass the same `quality` and `package` checks and remains subject to normal
review. Security updates may still be proposed independently of this version
update schedule.

## Release workflow

Manual release dispatch validates and publishes to TestPyPI. Publishing a
GitHub Release validates and publishes to PyPI. Release validation reuses both
commit-gate scripts, then adds Bandit, dependency auditing, reproducible double
builds, and publication.
