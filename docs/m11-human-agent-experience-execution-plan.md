# M11: Human and agent experience

M11 adds state-aware human output, detached waiting, portable agent
instructions, checked tutorials, and an optional local stdio MCP adapter. The
MCP server remains an interface above the existing orchestration operations;
it does not introduce a daemon, scheduler bypass, or HTTP service.

Completion notification uses renewable waits. CLI waits indefinitely unless a
timeout is supplied. MCP waits return after at most five minutes by default so
an agent host can renew the call without requiring human intervention.
