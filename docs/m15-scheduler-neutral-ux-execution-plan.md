# M15 - Scheduler-neutral user experience

## Objective

Make portable CLI operations and general setup documentation describe every
implemented scheduler backend accurately, without hiding real backend-specific
behavior.

## Milestones

1. Replace Slurm-only lifecycle help and generic target errors with portable
   scheduler terminology or explicit Slurm/OpenPBS alternatives.
2. Update installation, target setup, lifecycle, troubleshooting, and CLI
   reference documentation for both SSH/Slurm and SSH/OpenPBS stacks.
3. Preserve backend-specific names and guidance where they are contractual:
   `slurm_array`, Slurm worker pools, `resources.native.slurm`, Slurm preflight,
   and native scheduler command diagnostics.
4. Add regression tests for scheduler-neutral command help and asynchronous
   target errors, then run the complete source quality gate.

## Non-goals

- Rename existing JSON strategy values or backend-native configuration fields.
- Add OpenPBS worker-pool execution or change scheduler placement policy.
- Generalize the existing Slurm-specific remote preflight adapter.
- Change stable Run, Task, plan, or lifecycle JSON schemas.
