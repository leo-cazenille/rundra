# Agent capability doctor execution plan

## Objective

Make `rundr doctor` the safe first command for a new human or agent environment.
It detects real sandbox restrictions before submission, distinguishes known
readiness from complete verification, and generates least-privilege remediation
without changing security configuration.

## Milestones

1. Add doctor-v2 checks and typed filesystem, executable, network, socket, and
   remote requirements with reversible local write probes.
2. Add experiment-aware path resolution, live SSH/staging round trips, and a
   bounded optional scheduler probe through the scheduler abstraction.
3. Add stable human/JSON rendering and generated generic/Codex remediation.
4. Update agent guidance, public specifications, schema documentation, and
   fake-backend tests; keep real-cluster probes explicitly gated.

## Safety invariants

- Never output credential contents or weaken host verification.
- Never edit agent, SSH, target, or user configuration.
- Use private random probe paths and remove only exact known files/directories.
- Submit at most one explicitly requested 1-CPU probe job and cancel it on
  timeout or interruption.
- Do not fetch, pull, build, execute a container, or create a scientific Run.
