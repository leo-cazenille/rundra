# Shoal system testing

Shoal is Rundra's first real-cluster validation target. Its known path is a
local client through the `fishvision` SSH host, then Slurm and Apptainer on the
compute nodes, with `/shoalhome` shared between the login and compute nodes.

The checked target example is
[`examples/shoal/targets.yaml`](../examples/shoal/targets.yaml). Copy it to an
untracked location and replace `YOUR_USERNAME` with your Shoal username:

```bash
cp examples/shoal/targets.yaml /tmp/rundra-shoal-targets.yaml
sed -i "s/YOUR_USERNAME/$USER/" /tmp/rundra-shoal-targets.yaml
```

The `fishvision` name is an OpenSSH host alias, not embedded connection or
authentication configuration. Configure it in the normal user SSH files and
keep private keys, passwords, tokens, and other credentials out of Rundra
target, experiment, and RunRecord files. Host-key verification remains under
OpenSSH's normal policy and must not be disabled for testing.

The target deliberately does not prescribe a Slurm account, partition, QOS,
constraint, or GPU model. Those site/user-specific requests belong in the
experiment's explicit `resources.native.slurm` section and must follow Shoal
policy. The example also does not claim that the placeholder workspace exists,
is writable, or that any backend is reachable.

M4 system tests are opt-in and are documented here as they are introduced.
Ordinary `uv run pytest` must never contact Shoal or submit cluster work.
