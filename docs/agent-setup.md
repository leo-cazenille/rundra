# Agent target setup

Rundra never stores or brokers SSH credentials. A persistent user target file
can serve every project and agent session:

```text
~/.config/rundra/targets.yaml
```

Keep the adjacent project `rundra.yaml` non-secret: it may select a target by
name, while SSH host aliases, workspaces, accounts, partitions, and QOS remain
user/site configuration.

Sandboxes that expose a dedicated OpenSSH configuration can select it directly
in the user target file:

```yaml
transport:
  type: ssh
  host: cluster-login
  executable: /usr/bin/ssh
  config_file: /absolute/path/to/ssh/config
```

The config path is local to the Rundra client. Rundra passes it consistently to
OpenSSH and rsync without copying or inspecting private-key contents.

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

Start every new agent installation or sandbox with the bootstrap audit:

```bash
rundr doctor --agent codex --json
```

The audit performs real reversible writes in the effective Run store and local
preparation cache. Its structured requirements identify exact read/write paths,
network endpoints, executables, and SSH-agent sockets. The generated Codex TOML
is guidance only: Rundra never edits agent security configuration. After
granting only the reported permissions, start a new agent session and rerun the
audit because sandbox permissions are commonly fixed at session startup.

Check static setup without contacting the cluster:

```bash
rundr doctor experiment.yaml
```

After the sandbox has network and authentication access, request the live
staging probe:

```bash
rundr doctor experiment.yaml --connect --json
```

The live probe uses batch-mode SSH and creates a uniquely named private target
directory for a one-token upload/download round trip, then removes it. It
submits no scheduler work. Use `--scheduler-probe` only when one bounded no-op
job should verify scheduler acceptance and compute-side workspace access. If
sandbox policy cannot expose an authentication mechanism, a
human or external trusted execution broker must launch Rundra; Rundra does not
bypass that boundary.
