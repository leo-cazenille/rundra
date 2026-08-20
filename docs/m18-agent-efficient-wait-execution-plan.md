# M18: Agent-efficient waiting

## Objective

Keep human progress useful without streaming redundant terminal redraws into
LLM transcripts. Preserve one-document JSON stdout and daemon-free renewable
waiting.

## Implementation

1. Deduplicate unchanged progress observations and throttle changed redraws to
   a configurable interval while forcing phase and terminal updates.
2. Warn when explicit progress is captured alongside JSON and document silent
   blocking or bounded renewable waits as the agent path.
3. Add a local terminal completion alert to `wait`; do not add callbacks,
   credentials, persistent services, or scheduler-native notification logic.
4. Version the CLI surface and cover parsing, throttling, terminal behavior,
   and installed agent guidance with tests.
