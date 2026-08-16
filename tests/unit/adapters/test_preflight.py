from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

from rundra.adapters import RemotePreflight
from rundra.domain.models import (
    BackendConfig,
    Command,
    ContainerSpec,
    ExperimentSpec,
    ResourceRequest,
    Target,
)
from rundra.orchestration.preflight import PreflightStatus
from rundra.ports import CapabilityCheck, CommandResult
from tests.fakes import FakeTransport, RecordingContainerRuntime


def _target() -> Target:
    return Target(
        name="shoal",
        transport=BackendConfig("ssh", {"host": "fishvision"}),
        scheduler=BackendConfig("slurm"),
        staging=BackendConfig("rsync"),
        container=BackendConfig("apptainer"),
        workspace=PurePosixPath("/shoalhome/test/.rundra"),
    )


def _experiment(*, image: str = "/shoalhome/test/image.sif") -> ExperimentSpec:
    return ExperimentSpec(
        version=1,
        name="preflight",
        command=Command(
            ("python3", "main.py", "--config", "{config}", "--seed", "{seed}")
        ),
        resources=ResourceRequest(
            cpus_per_task=2,
            memory_bytes=1024**3,
            walltime=timedelta(minutes=5),
            native={"slurm": {"partition": "cpu"}},
        ),
        container=ContainerSpec(PurePosixPath(image)),
    )


def _result(
    command: Command,
    exit_code: int = 0,
    *,
    stdout: str = "",
    stderr: str = "",
) -> CommandResult:
    now = datetime(2026, 8, 15, tzinfo=UTC)
    return CommandResult(command, exit_code, stdout, stderr, now, now)


def _transport(
    exit_codes: tuple[int, ...], *, sacct_available: bool = True
) -> FakeTransport:
    commands = [Command(("scripted", str(index))) for index in range(len(exit_codes))]
    results = [
        _result(command, code)
        for command, code in zip(commands, exit_codes, strict=True)
    ]
    if len(results) > 2:
        slurm = results[2]
        results[2] = _result(
            slurm.command,
            slurm.exit_code,
            stdout=f"{str(sacct_available).lower()}\n",
        )
    if results:
        last = results[-1]
        results[-1] = _result(last.command, last.exit_code, stdout="zfs\n")
    return FakeTransport(
        deque((CapabilityCheck("ssh"),)),
        deque(results),
    )


def _runtime() -> RecordingContainerRuntime:
    return RecordingContainerRuntime(CapabilityCheck("apptainer"), Command(("true",)))


def test_remote_preflight_checks_every_layer_without_submitting() -> None:
    transport = _transport((0, 0, 0, 0, 0, 0))
    preflight = RemotePreflight(
        _target(),
        _experiment(),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    )

    report = preflight.run()

    assert report.ok
    assert [check.name for check in report.checks] == [
        "target_configuration",
        "ssh_client",
        "rsync_client",
        "ssh_connectivity",
        "workspace",
        "slurm_commands",
        "apptainer_runtime",
        "container_image",
        "requested_resources",
        "shared_filesystem",
    ]
    workspace_command = transport.run_calls[1]
    assert workspace_command.argv[:2] == ("/bin/sh", "-c")
    assert "while [ ! -e" in workspace_command.argv[2]
    assert "mkdir" not in workspace_command.argv[2]
    resource_command = transport.run_calls[-2]
    assert resource_command.argv[2].count("--test-only") == 1
    assert "--parsable" not in resource_command.argv[2]
    assert "#SBATCH --cpus-per-task=2" in resource_command.argv[4]
    assert "#SBATCH --partition=cpu" in resource_command.argv[4]
    assert all(
        "sbatch --parsable" not in " ".join(call.argv) for call in transport.run_calls
    )
    filesystem = report.checks[-1]
    assert filesystem.details == {"filesystem_type": "zfs"}
    filesystem_command = transport.run_calls[-1]
    assert "while [ ! -e" in filesystem_command.argv[2]
    assert "exec stat -f" in filesystem_command.argv[2]
    assert "mkdir" not in filesystem_command.argv[2]
    slurm = next(check for check in report.checks if check.name == "slurm_commands")
    assert slurm.details == {"sacct_available": True}


def test_remote_preflight_accepts_absent_optional_sacct() -> None:
    transport = _transport((0, 0, 0, 0, 0, 0), sacct_available=False)

    report = RemotePreflight(
        _target(),
        _experiment(),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    ).run()

    assert report.ok
    slurm = next(check for check in report.checks if check.name == "slurm_commands")
    assert slurm.details == {"sacct_available": False}
    command = transport.run_calls[2]
    assert "for name in sbatch squeue scancel scontrol" in command.argv[2]
    assert "command -v -- sacct" in command.argv[2]


def test_connectivity_failure_blocks_remote_checks_and_redacts_diagnostics() -> None:
    transport = _transport((255,))
    transport.run_script[0] = _result(
        Command(("scripted", "0")),
        255,
        stderr="secret remote diagnostic",
    )
    report = RemotePreflight(
        _target(),
        _experiment(),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    ).run()

    assert not report.ok
    connectivity = report.checks[3]
    assert connectivity.status is PreflightStatus.FAILED
    assert connectivity.details == {"exit_code": 255}
    assert "secret" not in connectivity.message
    assert all(check.status is PreflightStatus.BLOCKED for check in report.checks[4:])
    assert len(transport.run_calls) == 1


def test_relative_image_is_actionable_and_never_passed_to_remote_command() -> None:
    transport = _transport((0, 0, 0, 0, 0))

    report = RemotePreflight(
        _target(),
        _experiment(image="images/test.sif"),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    ).run()

    image = next(check for check in report.checks if check.name == "container_image")
    assert image.status is PreflightStatus.FAILED
    assert image.corrective_action is not None
    assert image.details == {"image": "images/test.sif"}
    assert all("images/test.sif" not in call.argv for call in transport.run_calls)


def test_workspace_failure_names_layer_action_and_only_safe_exit_detail() -> None:
    transport = _transport((0, 7, 0, 0, 0))

    report = RemotePreflight(
        _target(),
        _experiment(),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    ).run()

    workspace = next(check for check in report.checks if check.name == "workspace")
    assert workspace.layer == "staging"
    assert workspace.status is PreflightStatus.FAILED
    assert workspace.details == {"exit_code": 7}
    assert "writable" in (workspace.corrective_action or "")
    filesystem = next(
        check for check in report.checks if check.name == "shared_filesystem"
    )
    assert filesystem.status is PreflightStatus.BLOCKED
    assert filesystem.details == {"dependency": "workspace"}


def test_unsupported_target_blocks_all_adapter_calls() -> None:
    target = Target(
        name="local",
        transport=BackendConfig("local"),
        scheduler=BackendConfig("local"),
        staging=BackendConfig("local"),
        container=BackendConfig("native"),
        workspace=PurePosixPath("/tmp/rundra"),
    )
    transport = _transport(())

    report = RemotePreflight(
        target,
        _experiment(),
        transport,
        rsync_check=lambda: CapabilityCheck("rsync"),
        runtime=_runtime(),
    ).run()

    assert not report.ok
    assert report.checks[0].status is PreflightStatus.FAILED
    assert all(check.status is PreflightStatus.BLOCKED for check in report.checks[1:])
    assert transport.check_calls == 0
    assert transport.run_calls == []
