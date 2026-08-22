# M22 - Scheduler capabilities and OpenPBS workers

## Objective

Release v0.1.5 with a capability-driven scheduler boundary, scalable OpenPBS
worker pools, and local/remote offline preparation diagnostics. No additional
scheduler kind is introduced.

## Milestones

1. Add immutable scheduler capability metadata and a central adapter registry.
2. Publish derived capabilities in target-shaped JSON and plan schema v8.
3. Implement compact OpenPBS workers using the portable TaskSpace journals.
4. Probe offline preparation inputs at the selected local or target location.
5. Add adapter conformance and Docker OpenPBS lifecycle coverage.

OpenPBS supports compact workers but not scheduler-driven rerun recovery;
OpenPBS worker policies therefore require `requeue_limit: 0`.
