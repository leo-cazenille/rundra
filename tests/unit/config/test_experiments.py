from datetime import timedelta
from pathlib import Path, PurePosixPath

import pytest


def test_load_experiment_reads_a_minimal_version_one_document(tmp_path: Path) -> None:
    """Catches a loader that cannot construct the portable v1 domain value."""
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        """\
version: 1
experiment:
  name: example
command:
  argv: [python, main.py]
resources: {}
""",
        encoding="utf-8",
    )

    spec = load_experiment(source)

    assert spec.version == 1
    assert spec.name == "example"
    assert spec.command.argv == ("python", "main.py")
    assert spec.resources.nodes == 1
    assert spec.container is None
    assert spec.outputs == ()
    assert spec.sync_excludes == ()


def test_load_experiment_reports_missing_files_as_structured_errors(
    tmp_path: Path,
) -> None:
    """Catches leaking a raw OSError without an actionable error code or source."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "missing.yaml"
    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "CONFIG_NOT_FOUND"
    assert caught.value.path == ()
    assert caught.value.source == source
    assert "does not exist" in caught.value.message


@pytest.mark.parametrize(
    "setup, code",
    [
        ("directory", "CONFIG_IO"),
        ("invalid_utf8", "INVALID_ENCODING"),
    ],
)
def test_load_experiment_translates_unreadable_inputs(
    tmp_path: Path,
    setup: str,
    code: str,
) -> None:
    """Catches leaking platform I/O and decoding exceptions from the public loader."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    if setup == "directory":
        source.mkdir()
    else:
        source.write_bytes(b"\xff\xfe")

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == code
    assert caught.value.source == source


@pytest.mark.parametrize(
    "content, code",
    [
        ("version: [\n", "INVALID_YAML"),
        ("version: 1\nversion: 1\n", "DUPLICATE_FIELD"),
        ("---\nversion: 1\n---\nversion: 1\n", "INVALID_YAML"),
    ],
)
def test_load_experiment_rejects_ambiguous_or_malformed_yaml(
    tmp_path: Path,
    content: str,
    code: str,
) -> None:
    """Catches accepting syntax whose effective configuration is ambiguous."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == code
    assert caught.value.source == source


def test_load_experiment_normalizes_the_complete_portable_schema(
    tmp_path: Path,
) -> None:
    """Catches dropping portable fields or retaining scheduler-formatted units."""
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        """\
version: 1
experiment:
  name: collective-departure
command:
  argv:
    - python
    - main.py
    - --config
    - "{config}"
    - --seed
    - "{seed}"
  environment:
    MODE: test
  working_directory: project
container:
  image: containers/project.sif
  gpu: true
resources:
  nodes: 2
  tasks: 4
  cpus_per_task: 8
  gpus_per_task: 1
  memory: 16GiB
  walltime: "02:03:04"
  native:
    slurm:
      partition: gpu
      exclusive: true
outputs:
  include: ["results/**", "logs/**"]
sync:
  exclude: [".git/", ".venv/"]
""",
        encoding="utf-8",
    )

    spec = load_experiment(source)

    assert spec.command.environment == {"MODE": "test"}
    assert spec.command.working_directory == PurePosixPath("project")
    assert spec.container is not None
    assert spec.container.image == PurePosixPath("containers/project.sif")
    assert spec.container.gpu is True
    assert spec.resources.nodes == 2
    assert spec.resources.tasks == 4
    assert spec.resources.cpus_per_task == 8
    assert spec.resources.gpus_per_task == 1
    assert spec.resources.memory_bytes == 16 * 1024**3
    assert spec.resources.walltime == timedelta(hours=2, minutes=3, seconds=4)
    assert spec.resources.native == {"slurm": {"partition": "gpu", "exclusive": True}}
    assert spec.outputs == ("results/**", "logs/**")
    assert spec.sync_excludes == (".git/", ".venv/")


@pytest.mark.parametrize(
    "content, code, path",
    [
        (
            "version: 2\nexperiment: {name: x}\ncommand: {argv: [x]}\nresources: {}\n",
            "UNSUPPORTED_VERSION",
            ("version",),
        ),
        ("[]\n", "INVALID_TYPE", ()),
        (
            "version: 1\nexperiment: {name: x}\nresources: {}\n",
            "MISSING_FIELD",
            ("command",),
        ),
        (
            "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
            "resources: {}\nunexpected: true\n",
            "UNKNOWN_FIELD",
            ("unexpected",),
        ),
        (
            "version: 1\nexperiment: {name: x, extra: true}\n"
            "command: {argv: [x]}\nresources: {}\n",
            "UNKNOWN_FIELD",
            ("experiment", "extra"),
        ),
        (
            "version: 1\nexperiment: {name: true}\ncommand: {argv: [x]}\n"
            "resources: {}\n",
            "INVALID_TYPE",
            ("experiment", "name"),
        ),
    ],
)
def test_load_experiment_reports_schema_errors_with_precise_paths(
    tmp_path: Path,
    content: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    """Catches permissive schemas and errors that do not identify the bad field."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(content, encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == code
    assert caught.value.path == path


@pytest.mark.parametrize(
    "field, value",
    [
        ("memory", "16GB"),
        ("memory", "0GiB"),
        ("walltime", "2:00:00"),
        ("walltime", "02:60:00"),
    ],
)
def test_load_experiment_rejects_invalid_resource_units(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    """Catches silently reinterpreting unsupported memory or duration syntax."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        f"resources: {{{field}: {value!r}}}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "INVALID_VALUE"
    assert caught.value.path == ("resources", field)


@pytest.mark.parametrize(
    "value, expected",
    [
        ("1B", 1),
        ("2KiB", 2 * 1024),
        ("3MiB", 3 * 1024**2),
        ("4GiB", 4 * 1024**3),
        ("5TiB", 5 * 1024**4),
    ],
)
def test_load_experiment_normalizes_each_supported_memory_unit(
    tmp_path: Path,
    value: str,
    expected: int,
) -> None:
    """Catches wrong binary-unit factors or accidental decimal-unit conversion."""
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        f"resources: {{memory: {value}}}\n",
        encoding="utf-8",
    )

    assert load_experiment(source).resources.memory_bytes == expected


@pytest.mark.parametrize(
    "fragment, code, path",
    [
        ("command: {argv: []}", "INVALID_VALUE", ("command", "argv")),
        (
            "command: {argv: [x], environment: {COUNT: 1}}",
            "INVALID_TYPE",
            ("command", "environment", "COUNT"),
        ),
        (
            "command: {argv: [x], environment: {API_TOKEN: forbidden}}",
            "FORBIDDEN_FIELD",
            ("command", "environment", "API_TOKEN"),
        ),
        ("resources: {nodes: 0}", "INVALID_VALUE", ("resources", "nodes")),
        ("resources: {tasks: true}", "INVALID_TYPE", ("resources", "tasks")),
        (
            "resources: {native: {partition: gpu}}",
            "INVALID_TYPE",
            ("resources", "native", "partition"),
        ),
    ],
)
def test_load_experiment_translates_nested_value_errors(
    tmp_path: Path,
    fragment: str,
    code: str,
    path: tuple[str | int, ...],
) -> None:
    """Catches raw domain exceptions and non-namespaced native options."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    command = "command: {argv: [x]}"
    resources = "resources: {}"
    if fragment.startswith("command:"):
        command = fragment
    else:
        resources = fragment
    source = tmp_path / "experiment.yaml"
    source.write_text(
        f"version: 1\nexperiment: {{name: x}}\n{command}\n{resources}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == code
    assert caught.value.path == path


def test_load_experiment_rejects_credentials_hidden_in_native_options(
    tmp_path: Path,
) -> None:
    """Catches persisting credentials through auditable native resource options."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        """\
version: 1
experiment: {name: x}
command: {argv: [x]}
resources:
  native:
    slurm:
      access_token: forbidden
""",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "FORBIDDEN_FIELD"
    assert caught.value.path == ("resources", "native", "slurm", "access_token")


def test_load_config_snapshot_preserves_exact_opaque_yaml(tmp_path: Path) -> None:
    """Catches interpreting or re-serializing application-specific configuration."""
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    content = "# keep this comment\npopulation:\n  size: 100\nnoise: 0.10\n"
    source.write_text(content, encoding="utf-8")

    snapshot = load_config_snapshot(source)

    assert snapshot.source == source
    assert snapshot.content == content


@pytest.mark.parametrize(
    "content",
    [b"population:\r\n  size: 100\r\n", b"population:\n  size: 100"],
)
def test_load_config_snapshot_preserves_newlines_and_end_of_file_exactly(
    tmp_path: Path,
    content: bytes,
) -> None:
    """Catches universal-newline conversion or adding a final newline."""
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_bytes(content)

    assert load_config_snapshot(source).content == content.decode("utf-8")


def test_load_config_snapshot_rejects_invalid_yaml_without_interpreting_schema(
    tmp_path: Path,
) -> None:
    """Catches accepting malformed input while still permitting arbitrary keys."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text("application_specific: [\n", encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_config_snapshot(source)

    assert caught.value.code == "INVALID_YAML"
    assert caught.value.path == ()


def test_load_config_snapshot_translates_complex_mapping_key_errors(
    tmp_path: Path,
) -> None:
    """Catches leaking an unhashable-key TypeError through the YAML boundary."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text("? [a, b]\n: value\n", encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_config_snapshot(source)

    assert caught.value.code == "INVALID_TYPE"
    assert caught.value.path == ()


def test_load_config_snapshot_reports_the_full_nested_duplicate_path(
    tmp_path: Path,
) -> None:
    """Catches losing the containing path for duplicate nested fields."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text("outer:\n  repeated: 1\n  repeated: 2\n", encoding="utf-8")

    with pytest.raises(ConfigError) as caught:
        load_config_snapshot(source)

    assert caught.value.code == "DUPLICATE_FIELD"
    assert caught.value.path == ("outer", "repeated")


@pytest.mark.parametrize(
    "field",
    ["API_KEY", "SECRET_KEY", "SSH_CREDENTIAL", "AUTHORIZATION"],
)
def test_load_experiment_rejects_common_credential_environment_names(
    tmp_path: Path,
    field: str,
) -> None:
    """Catches storing common credential fields in ExperimentSpec environment."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\n"
        f"command: {{argv: [x], environment: {{{field}: forbidden}}}}\n"
        "resources: {}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "FORBIDDEN_FIELD"
    assert caught.value.path == ("command", "environment", field)


def test_load_experiment_translates_walltime_overflow(tmp_path: Path) -> None:
    """Catches leaking OverflowError for syntactically valid extreme hours."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        "resources: {walltime: '999999999999999999999999:00:00'}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "INVALID_VALUE"
    assert caught.value.path == ("resources", "walltime")


@pytest.mark.parametrize("field", ["memory", "walltime"])
def test_load_experiment_translates_integer_digit_limit_errors(
    tmp_path: Path,
    field: str,
) -> None:
    """Catches leaking Python's integer conversion limit for huge unit values."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    value = "9" * 5000 + ("GiB" if field == "memory" else ":00:00")
    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        f"resources: {{{field}: '{value}'}}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "INVALID_VALUE"
    assert caught.value.path == ("resources", field)


def test_load_experiment_rejects_blank_native_namespaces(tmp_path: Path) -> None:
    """Catches native options that are not explicitly namespaced to a backend."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        "resources: {native: {'': {partition: gpu}}}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "INVALID_VALUE"
    assert caught.value.path == ("resources", "native", "")


@pytest.mark.parametrize("value", [".nan", ".inf", "-.inf"])
def test_load_experiment_rejects_nonfinite_native_numbers(
    tmp_path: Path, value: str
) -> None:
    """Keeps every accepted native option representable in strict JSON."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_experiment

    source = tmp_path / "experiment.yaml"
    source.write_text(
        "version: 1\nexperiment: {name: x}\ncommand: {argv: [x]}\n"
        f"resources: {{native: {{slurm: {{priority: {value}}}}}}}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_experiment(source)

    assert caught.value.code == "INVALID_VALUE"
    assert caught.value.path == ("resources", "native", "slurm", "priority")


@pytest.mark.parametrize(
    "content",
    ["loop: &loop [*loop]\n", "node: &node {self: *node}\n"],
)
def test_load_config_snapshot_accepts_safe_recursive_aliases(
    tmp_path: Path,
    content: str,
) -> None:
    """Catches duplicate-path validation changing SafeLoader YAML semantics."""
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text(content, encoding="utf-8")

    assert load_config_snapshot(source).content == content


@pytest.mark.parametrize(
    "content",
    [
        "defaults: &defaults {color: red}\nitem: {<<: *defaults, size: 1}\n",
        (
            "first: &first {color: red}\nsecond: &second {size: 1}\n"
            "item: {<<: [*first, *second]}\n"
        ),
        "defaults: &defaults {color: red}\nitem: {<<: *defaults, color: blue}\n",
    ],
)
def test_load_config_snapshot_preserves_safe_yaml_merge_semantics(
    tmp_path: Path,
    content: str,
) -> None:
    """Catches duplicate validation preempting SafeLoader merge flattening."""
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text(content, encoding="utf-8")

    assert load_config_snapshot(source).content == content


def test_load_config_snapshot_rejects_duplicate_merge_keys(tmp_path: Path) -> None:
    """Catches merge tags bypassing the strict duplicate-key rule."""
    from rundra.config.errors import ConfigError
    from rundra.config.experiments import load_config_snapshot

    source = tmp_path / "scientific.yaml"
    source.write_text(
        "item:\n  <<: {color: red}\n  <<: {size: 1}\n",
        encoding="utf-8",
    )

    with pytest.raises(ConfigError) as caught:
        load_config_snapshot(source)

    assert caught.value.code == "DUPLICATE_FIELD"
    assert caught.value.path == ("item", "<<")
