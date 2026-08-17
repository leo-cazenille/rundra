# Agent target setup

Rundra never stores or brokers SSH credentials. A persistent user target file
can serve every project and agent session:

```text
~/.config/rundra/targets.yaml
```

Keep the adjacent project `rundra.yaml` non-secret: it may select a target by
name, while SSH host aliases, workspaces, accounts, partitions, and QOS remain
user/site configuration.

## Safe sandbox prerequisites

An agent sandbox that launches remote Runs needs:

- read access to the Rundra target and user configuration files;
- read access to the normal OpenSSH config and known-hosts files;
- the inherited `SSH_AUTH_SOCK` socket when SSH-agent authentication is used;
- network access to the configured login host;
- write access to the project retrieval and RunRecord directories.

Expose the agent socket, not private key files. Do not copy private keys into a
project or temporary agent home, disable host verification, or place
credentials in Rundra YAML. Merely changing `HOME` is not a reliable OpenSSH
setup because OpenSSH may derive user paths from the operating-system account.

Check static setup without contacting the cluster:

```bash
uv run rundr doctor examples/pogosim-shoal/experiment.yaml
```

After the sandbox has network and authentication access, request the read-only
live probe:

```bash
uv run rundr doctor examples/pogosim-shoal/experiment.yaml --connect --json
```

The live probe uses batch-mode SSH, submits no scheduler work, and creates no
remote state. If sandbox policy cannot expose an authentication mechanism, a
human or external trusted execution broker must launch Rundra; Rundra does not
bypass that boundary.
