from __future__ import annotations

from pathlib import Path

from rundra.cli.capability_doctor import doctor_operation
from rundra.cli.render import render_json


def test_doctor_accepts_a_writable_local_target(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  local:
    transport: {{type: local}}
    scheduler: {{type: local}}
    staging: {{type: local}}
    container: {{type: native}}
    workspace: {tmp_path / "workspace"}
""",
        encoding="utf-8",
    )

    result = doctor_operation(targets, "local")

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.format_version == 2
    assert any(check.name == "workspace" for check in result.value.checks)
    assert not (tmp_path / "workspace").exists()


def test_doctor_reports_missing_target_without_connecting(tmp_path: Path) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text("version: 1\ntargets: {}\n", encoding="utf-8")

    result = doctor_operation(targets, "absent", connect=True)

    assert result.ok and result.value is not None
    assert not result.value.ready
    assert any(check.name == "selected_target" for check in result.value.checks)


def test_bootstrap_doctor_reports_paths_and_generates_codex_profile(
    tmp_path: Path,
) -> None:
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        "\n".join(
            (
                "version: 1",
                "targets:",
                "  local:",
                "    transport: {type: local}",
                "    scheduler: {type: local}",
                "    staging: {type: local}",
                "    container: {type: native}",
                f"    workspace: {tmp_path / 'workspace'}",
                "",
            )
        ),
        encoding="utf-8",
    )
    data_dir = tmp_path / "records"
    cache = tmp_path / "cache"

    result = doctor_operation(
        targets, None, data_dir=data_dir, cache_root=cache, agent="codex"
    )

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.mode == "bootstrap"
    assert result.value.agent_config is not None
    assert "[permissions.rundra.filesystem]" in result.value.agent_config
    assert str(data_dir) in result.value.agent_config
    assert not data_dir.exists()
    assert not cache.exists()
    document = render_json(result)
    assert '"format_version":2' in document
    assert '"complete":false' in document


def test_doctor_rejects_scheduler_probe_without_write_probe(tmp_path: Path) -> None:
    result = doctor_operation(
        tmp_path / "targets.yaml",
        None,
        scheduler_probe=True,
        write_probe=False,
    )

    assert result.error is not None
    assert result.error.code == "CLI_USAGE_ERROR"


def test_doctor_requires_target_for_local_target_access(tmp_path: Path) -> None:
    result = doctor_operation(
        tmp_path / "targets.yaml",
        None,
        local_target_access=True,
    )

    assert result.error is not None
    assert result.error.code == "CLI_USAGE_ERROR"


def test_doctor_audits_explicit_local_access_to_remote_target(
    tmp_path: Path,
) -> None:
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory\n", encoding="utf-8")
    workspace = blocker / "workspace"
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 1
targets:
  cluster:
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: rsync}}
    container: {{type: apptainer}}
    workspace: {workspace}
""",
        encoding="utf-8",
    )

    result = doctor_operation(
        targets,
        "cluster",
        local_target_access=True,
        agent="codex",
    )

    assert result.ok and result.value is not None
    assert not result.value.ready
    assert result.value.agent_config is not None
    assert str(workspace) in result.value.agent_config
    assert str(workspace / "cache") in result.value.agent_config
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["local_target_workspace"] == "fail"
    assert checks["local_target_preparation_cache"] == "fail"


def test_doctor_automatically_audits_shared_target_paths(tmp_path: Path) -> None:
    shared = tmp_path / "shared"
    workspace = shared / "workspace"
    image_search = shared / "images"
    image_search.mkdir(parents=True)
    targets = tmp_path / "targets.yaml"
    targets.write_text(
        f"""version: 6
targets:
  cluster:
    transport: {{type: ssh, host: cluster}}
    scheduler: {{type: slurm}}
    staging: {{type: shared, root: {shared}}}
    container: {{type: apptainer}}
    workspace: {workspace}
    preparation:
      cache_root: {workspace / "prepared"}
      image_search_paths: [{image_search}]
    execution:
      hard_task_limit: 1000
      confirmation_threshold: 1000
      max_active_tasks: 40
      max_concurrent_jobs: 2
      max_array_size: 1000
      output_shard_tasks: 100
      automatic_retrieval_threshold: 1000
      worker_pool:
        activation_threshold: 100
        default_workers: 1
        max_workers: 2
        default_task_slots_per_worker: 1
        max_task_slots_per_worker: 20
        tasks_per_lease: 20
        infrastructure_retry_limit: 1
        requeue_limit: 2
""",
        encoding="utf-8",
    )

    result = doctor_operation(targets, "cluster", agent="codex")

    assert result.ok and result.value is not None
    assert result.value.ready
    assert result.value.agent_config is not None
    checks = {check.name: check.status for check in result.value.checks}
    assert checks["local_target_workspace"] == "pass"
    assert checks["local_target_preparation_cache"] == "pass"
    assert checks["local_target_image_search_path_0"] == "pass"
    assert str(workspace) in result.value.agent_config
    assert str(workspace / "prepared") in result.value.agent_config
    assert str(image_search) in result.value.agent_config
    assert not workspace.exists()
