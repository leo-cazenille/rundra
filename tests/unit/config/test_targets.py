from pathlib import Path, PurePosixPath

import pytest


def test_load_targets_builds_immutable_site_configuration(tmp_path: Path) -> None:
    """Catches mixing target selections/options into scientific configuration."""
    from rundra.config.targets import load_targets

    source = tmp_path / "targets.yaml"
    source.write_text(
        """\
version: 1
targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: apptainer}
    workspace: ~/.local/share/rundra
  shoal:
    transport:
      type: ssh
      host: fishvision
      executable: /usr/bin/ssh
      config_file: /etc/rundra/ssh_config
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /shoalhome/{user}/.rundra
""",
        encoding="utf-8",
    )

    targets = load_targets(source)

    assert tuple(targets) == ("local", "shoal")
    assert targets["local"].workspace == PurePosixPath("~/.local/share/rundra")
    assert targets["shoal"].transport.kind == "ssh"
    assert targets["shoal"].transport.options == {
        "host": "fishvision",
        "executable": "/usr/bin/ssh",
        "config_file": "/etc/rundra/ssh_config",
    }
    assert targets["shoal"].scheduler.kind == "slurm"
    assert targets["shoal"].workspace == PurePosixPath("/shoalhome/{user}/.rundra")
    with pytest.raises(TypeError):
        targets["other"] = targets["local"]


def test_version_two_target_preparation_storage_is_separate_and_strict(
    tmp_path: Path,
) -> None:
    from rundra.config.targets import load_targets_config

    source = tmp_path / "targets.yaml"
    source.write_text(
        """\
version: 2
targets:
  shoal:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /remote/work
    preparation:
      cache_root: /shared/rundra/cache
      image_search_paths: [/shared/images, /opt/images]
""",
        encoding="utf-8",
    )

    config = load_targets_config(source)

    assert config.version == 2
    assert "preparation" not in config.targets["shoal"].transport.options
    storage = config.preparation["shoal"]
    assert storage.cache_root == PurePosixPath("/shared/rundra/cache")
    assert storage.image_search_paths == (
        PurePosixPath("/shared/images"),
        PurePosixPath("/opt/images"),
    )


def test_version_three_target_requires_explicit_execution_policy(
    tmp_path: Path,
) -> None:
    from rundra.config.targets import load_targets_config

    source = tmp_path / "targets.yaml"
    source.write_text(
        """\
version: 3
targets:
  shoal:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: apptainer}
    workspace: /remote/work
    execution:
      hard_task_limit: 100000000
      confirmation_threshold: 10000
      max_active_tasks: 800
      max_array_size: 1001
      output_shard_tasks: 1000
      automatic_retrieval_threshold: 20000
      worker_pool:
        activation_threshold: 100000
        max_workers: 64
        tasks_per_lease: 100
        infrastructure_retry_limit: 2
        requeue_limit: 8
""",
        encoding="utf-8",
    )

    config = load_targets_config(source)

    policy = config.execution["shoal"]
    assert config.version == 3
    assert policy.hard_task_limit == 100_000_000
    assert policy.max_active_tasks == 800
    assert policy.max_concurrent_jobs == 256
    assert policy.worker_pool.max_workers == 64
    assert policy.worker_pool.tasks_per_lease == 100


def test_version_three_target_rejects_missing_or_inconsistent_policy(
    tmp_path: Path,
) -> None:
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets_config

    missing = tmp_path / "missing.yaml"
    missing.write_text(
        "version: 3\ntargets:\n  local:\n"
        "    transport: {type: local}\n    scheduler: {type: local}\n"
        "    staging: {type: local}\n    container: {type: native}\n"
        "    workspace: /tmp/rundra\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as caught:
        load_targets_config(missing)
    assert caught.value.code == "MISSING_FIELD"
    assert caught.value.path == ("targets", "local", "execution")

    inconsistent = tmp_path / "inconsistent.yaml"
    inconsistent.write_text(
        """\
version: 3
targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
    workspace: /tmp/rundra
    execution:
      hard_task_limit: 10
      confirmation_threshold: 11
      max_active_tasks: 2
      max_array_size: 2
      output_shard_tasks: 2
      automatic_retrieval_threshold: 2
      worker_pool:
        activation_threshold: 3
        max_workers: 1
        tasks_per_lease: 1
        infrastructure_retry_limit: 0
        requeue_limit: 0
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as caught:
        load_targets_config(inconsistent)
    assert caught.value.code == "INVALID_EXECUTION_POLICY"


@pytest.mark.parametrize(
    "content, code, path",
    [
        ("version: 4\ntargets: {}\n", "UNSUPPORTED_VERSION", ("version",)),
        ("version: 1\n", "MISSING_FIELD", ("targets",)),
        (
            "version: 1\ntargets: []\n",
            "INVALID_TYPE",
            ("targets",),
        ),
        (
            "version: 1\ntargets:\n  local:\n    transport: {type: local}\n"
            "    scheduler: {type: local}\n    staging: {type: local}\n"
            "    container: {type: apptainer}\n    workspace: /tmp\n"
            "    partition: cpu\n",
            "UNKNOWN_FIELD",
            ("targets", "local", "partition"),
        ),
        (
            "version: 1\ntargets:\n  bad:\n    transport: {type: telnet}\n"
            "    scheduler: {type: local}\n    staging: {type: local}\n"
            "    container: {type: apptainer}\n    workspace: /tmp\n",
            "UNKNOWN_BACKEND",
            ("targets", "bad", "transport", "type"),
        ),
        (
            "version: 1\ntargets:\n  bad:\n    transport: {type: ssh}\n"
            "    scheduler: {type: local}\n    staging: {type: local}\n"
            "    container: {type: apptainer}\n    workspace: /tmp\n",
            "MISSING_FIELD",
            ("targets", "bad", "transport", "host"),
        ),
        (
            "version: 1\ntargets:\n  bad:\n"
            "    transport: {type: ssh, host: x, password: no}\n"
            "    scheduler: {type: local}\n    staging: {type: local}\n"
            "    container: {type: apptainer}\n    workspace: /tmp\n",
            "FORBIDDEN_FIELD",
            ("targets", "bad", "transport", "password"),
        ),
        (
            "version: 1\ntargets:\n  bad:\n"
            "    transport: {type: ssh, host: '-oProxyCommand=danger'}\n"
            "    scheduler: {type: slurm}\n    staging: {type: rsync}\n"
            "    container: {type: apptainer}\n    workspace: /tmp/rundra\n",
            "INVALID_VALUE",
            ("targets", "bad", "transport", "host"),
        ),
    ],
)
def test_load_targets_reports_strict_actionable_schema_errors(
    tmp_path: Path,
    content: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    """Catches permissive site schemas or backend errors without field paths."""
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets

    source = tmp_path / "targets.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_targets(source)

    assert caught.value.code == code
    assert caught.value.path == path


@pytest.mark.parametrize(
    "role, kind",
    [
        ("transport", "slurm"),
        ("scheduler", "ssh"),
        ("staging", "apptainer"),
        ("container", "local"),
    ],
)
def test_load_targets_rejects_known_backend_in_the_wrong_role(
    tmp_path: Path,
    role: str,
    kind: str,
) -> None:
    """Catches treating backend names as interchangeable plugin identifiers."""
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets

    sections = {
        "transport": "local",
        "scheduler": "local",
        "staging": "local",
        "container": "apptainer",
    }
    sections[role] = kind
    source = tmp_path / "targets.yaml"
    source.write_text(
        "version: 1\ntargets:\n  bad:\n"
        + "".join(
            f"    {name}: {{type: {value}}}\n" for name, value in sections.items()
        )
        + "    workspace: /tmp\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_targets(source)

    assert caught.value.code == "UNKNOWN_BACKEND"
    assert caught.value.path == ("targets", "bad", role, "type")


def test_native_runtime_is_explicit_and_limited_to_an_all_local_target(
    tmp_path: Path,
) -> None:
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets

    local = tmp_path / "local.yaml"
    local.write_text(
        """\
version: 1
targets:
  local:
    transport: {type: local}
    scheduler: {type: local}
    staging: {type: local}
    container: {type: native}
    workspace: /tmp/rundra
""",
        encoding="utf-8",
    )
    assert load_targets(local)["local"].container.kind == "native"

    remote = tmp_path / "remote.yaml"
    remote.write_text(
        """\
version: 1
targets:
  bad:
    transport: {type: ssh, host: cluster}
    scheduler: {type: slurm}
    staging: {type: rsync}
    container: {type: native}
    workspace: /remote/rundra
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as caught:
        load_targets(remote)
    assert caught.value.code == "INVALID_BACKEND_COMBINATION"
    assert caught.value.path == ("targets", "bad")


@pytest.mark.parametrize(
    ("transport", "scheduler", "staging", "container"),
    [
        ("local", "slurm", "local", "apptainer"),
        ("ssh", "local", "rsync", "apptainer"),
        ("ssh", "slurm", "local", "apptainer"),
        ("local", "local", "rsync", "apptainer"),
    ],
)
def test_target_schema_rejects_unexecutable_backend_mixtures(
    tmp_path: Path,
    transport: str,
    scheduler: str,
    staging: str,
    container: str,
) -> None:
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets

    transport_document = (
        "{type: ssh, host: cluster}" if transport == "ssh" else "{type: local}"
    )
    source = tmp_path / "targets.yaml"
    source.write_text(
        "version: 1\ntargets:\n  bad:\n"
        f"    transport: {transport_document}\n"
        f"    scheduler: {{type: {scheduler}}}\n"
        f"    staging: {{type: {staging}}}\n"
        f"    container: {{type: {container}}}\n"
        "    workspace: /tmp/rundra\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_targets(source)

    assert caught.value.code == "INVALID_BACKEND_COMBINATION"
    assert caught.value.path == ("targets", "bad")


@pytest.mark.parametrize("workspace", ["relative/workspace", "/"])
def test_ssh_target_requires_an_absolute_non_root_workspace(
    tmp_path: Path, workspace: str
) -> None:
    from rundra.config.errors import ConfigError
    from rundra.config.targets import load_targets

    source = tmp_path / "targets.yaml"
    source.write_text(
        "version: 1\ntargets:\n  bad:\n"
        "    transport: {type: ssh, host: cluster}\n"
        "    scheduler: {type: slurm}\n"
        "    staging: {type: rsync}\n"
        "    container: {type: apptainer}\n"
        f"    workspace: {workspace}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_targets(source)

    assert caught.value.code == "INVALID_REMOTE_WORKSPACE"
    assert caught.value.path == ("targets", "bad", "workspace")
