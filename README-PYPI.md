# Rundra

Rundra is a portable experiment-execution framework for reproducible scientific
computing. It provides stable run and task identities, explicit stochastic
seeds, structured JSON output, immutable staged inputs, scheduler integration,
and result retrieval through one command-line interface.

## Installation

Rundra requires Python 3.12. Install the command as an isolated user tool:

```bash
uv tool install rundra
rundr --version
rundr help
```

Install the optional MCP interface with:

```bash
uv tool install 'rundra[mcp]'
rundr-mcp --help
```

## Basic workflow

An experiment combines an executable command, a YAML configuration, explicit
resources, and one or more integer seeds:

```bash
rundr validate experiment.yaml
rundr plan experiment.yaml --seeds 0:3
rundr submit experiment.yaml --seeds 0:3
rundr wait RUN_ID --progress
rundr fetch RUN_ID
```

`rundr run` performs submission, waiting, and retrieval synchronously. Remote
targets can combine SSH transport, a Slurm, OpenPBS, or HTCondor scheduler,
rsync or shared staging, and an Apptainer container runtime. HTCondor currently
requires an explicitly shared workspace and supports reliable vanilla Task
clusters without dependencies or compact workers. Site connection, account,
queue, workspace, and authentication policy remain explicit operator
configuration.

If an asynchronous submission is interrupted, `rundr resume RUN_ID` recovers
its durable scheduler outcome without creating a duplicate Run. `rundr list`
returns compact paginated summaries, while `rundr tasks RUN_ID` pages through
Task details. Automatic fetch can avoid redundant bulk transfer when client and
target paths are safely visible through the same filesystem.

## Automation

All important lifecycle commands support structured JSON through `--json`.
Agents should retain explicit Run IDs and use `rundr await RUN_ID... --json`
instead of interactive progress output or repeated model-driven polling.
The optional MCP
server exposes the same operation layer over stdio or authenticated Streamable
HTTP, including submission recovery and paginated discovery. Network transport
requires an environment-sourced bearer token and an explicit host policy; TLS
should terminate at an operator-managed reverse proxy.

Rundra does not store SSH keys, passwords, registry credentials, or scheduler
credentials. Run only experiment definitions and source trees that you trust,
because their declared commands execute with the configured user's privileges.

The source repository contains development documentation, examples, schemas,
and the complete test suite.
