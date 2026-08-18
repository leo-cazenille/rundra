from __future__ import annotations

import base64
import gzip
import subprocess
from collections import deque
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath

import pytest

from rundra.adapters import (
    SlurmArrayRequest,
    SlurmCancellationError,
    SlurmQueryError,
    SlurmScheduler,
    SlurmScriptError,
    SlurmSubmissionError,
    render_sbatch_array_script,
    render_sbatch_script,
    render_slurm_array_manifest,
)
from rundra.adapters.slurm import _portable_state
from rundra.domain.mappings import ArrayTaskMapping
from rundra.domain.models import Command, ResourceRequest, TaskId
from rundra.domain.states import ExecutionState
from rundra.ports import (
    CapabilityCheck,
    CommandResult,
    Scheduler,
    SchedulerArrayRequest,
    SchedulerGroup,
    SchedulerReference,
    SchedulerUnit,
)

_MANIFEST_PATH = PurePosixPath("/remote/run/metadata/tasks.sh")


class ScriptedTransport:
    def __init__(self, results: deque[CommandResult | Exception]) -> None:
        self.results = results
        self.run_calls: list[Command] = []

    def check(self) -> CapabilityCheck:
        return CapabilityCheck("scripted")

    def run(self, command: Command) -> CommandResult:
        self.run_calls.append(command)
        outcome = self.results.popleft()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _group(
    *,
    command: Command | None = None,
    resources: ResourceRequest | None = None,
    ordinal: int = 0,
) -> SchedulerGroup:
    return SchedulerGroup(
        (
            SchedulerUnit(
                TaskId.from_ordinal(ordinal),
                command or Command(("python3", "experiment.py")),
                resources or ResourceRequest(),
            ),
        )
    )


def _array_request(
    *,
    count: int = 3,
    resources: ResourceRequest | None = None,
    manifest_path: PurePosixPath = _MANIFEST_PATH,
    max_array_size: int = 1001,
) -> SlurmArrayRequest:
    units = tuple(
        SchedulerUnit(
            TaskId.from_ordinal(index),
            Command(("python3", "experiment.py", "--seed", str(index + 7))),
            resources or ResourceRequest(),
        )
        for index in range(count)
    )
    return SlurmArrayRequest(
        SchedulerGroup(units),
        tuple(
            ArrayTaskMapping(unit.task_id, index + 7, index)
            for index, unit in enumerate(units)
        ),
        manifest_path,
        max_array_size,
    )


def test_slurm_array_request_preserves_explicit_bounded_mapping() -> None:
    request = _array_request(count=3, max_array_size=3)

    assert [unit.task_id for unit in request.group.units] == [
        item.task_id for item in request.mapping
    ]
    assert [item.seed for item in request.mapping] == [7, 8, 9]
    assert [item.array_index for item in request.mapping] == [0, 1, 2]
    assert request.manifest_path == PurePosixPath("/remote/run/metadata/tasks.sh")
    assert request.max_array_size == 3


def test_slurm_array_request_rejects_invalid_task_and_index_mappings() -> None:
    request = _array_request()
    first, second, third = request.mapping

    with pytest.raises(SlurmScriptError, match="Task order"):
        SlurmArrayRequest(
            request.group,
            (second, first, third),
            request.manifest_path,
            request.max_array_size,
        )
    with pytest.raises(SlurmScriptError, match="contiguous and zero-based"):
        SlurmArrayRequest(
            request.group,
            (first, second, ArrayTaskMapping(third.task_id, third.seed, 7)),
            request.manifest_path,
            request.max_array_size,
        )
    with pytest.raises(SlurmScriptError, match="seeds must be unique"):
        SlurmArrayRequest(
            request.group,
            (first, ArrayTaskMapping(second.task_id, first.seed, 1), third),
            request.manifest_path,
            request.max_array_size,
        )


def test_slurm_array_request_rejects_heterogeneous_resources() -> None:
    request = _array_request(count=2)
    first, second = request.group.units
    heterogeneous = SchedulerGroup(
        (
            first,
            SchedulerUnit(
                second.task_id,
                second.command,
                ResourceRequest(cpus_per_task=8),
            ),
        )
    )

    with pytest.raises(SlurmScriptError, match="uniform resources"):
        SlurmArrayRequest(
            heterogeneous,
            request.mapping,
            request.manifest_path,
            request.max_array_size,
        )


@pytest.mark.parametrize("max_array_size", [0, -1, True, 1.5])
def test_slurm_array_request_rejects_invalid_array_bounds(
    max_array_size: object,
) -> None:
    if type(max_array_size) is int and max_array_size <= 0:
        expected = ValueError
    else:
        expected = TypeError
    with pytest.raises(expected, match="max_array_size"):
        _array_request(max_array_size=max_array_size)  # type: ignore[arg-type]

    with pytest.raises(SlurmScriptError, match="exceeds.*MaxArraySize"):
        _array_request(count=4, max_array_size=3)


@pytest.mark.parametrize(
    "manifest_path",
    [PurePosixPath("relative/tasks.sh"), PurePosixPath("/remote/bad\x00path")],
)
def test_slurm_array_request_rejects_unsafe_manifest_paths(
    manifest_path: PurePosixPath,
) -> None:
    with pytest.raises(SlurmScriptError, match="absolute and safe"):
        _array_request(manifest_path=manifest_path)


def test_slurm_array_request_requires_multiple_tasks() -> None:
    with pytest.raises(SlurmScriptError, match="at least two Tasks"):
        _array_request(count=1)


def test_array_manifest_is_deterministic_and_maps_each_explicit_task() -> None:
    request = _array_request(count=3)

    first = render_slurm_array_manifest(request)
    second = render_slurm_array_manifest(request)

    assert first == second
    assert first.startswith("#!/bin/sh\nset -eu\n")
    assert 'case "$1" in' in first
    for item in request.mapping:
        assert f"  {item.array_index})" in first
        assert f"# task_id={item.task_id} seed={item.seed}" in first
    assert "eval" not in first
    assert "sh -c" not in first


def test_array_manifest_dispatches_only_the_selected_literal_command(
    tmp_path: PurePosixPath,
) -> None:
    units = tuple(
        SchedulerUnit(
            TaskId.from_ordinal(index),
            Command(("printf", "%s\n", f"task-{index}")),
            ResourceRequest(),
        )
        for index in range(2)
    )
    request = SlurmArrayRequest(
        SchedulerGroup(units),
        tuple(
            ArrayTaskMapping(unit.task_id, 10 + index, index)
            for index, unit in enumerate(units)
        ),
        tmp_path / "tasks.sh",
        2,
    )
    manifest = render_slurm_array_manifest(request)

    first = subprocess.run(
        ("/bin/sh", "-c", manifest, "rundra-array-manifest", "0"),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )
    second = subprocess.run(
        ("/bin/sh", "-c", manifest, "rundra-array-manifest", "1"),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert (first.returncode, first.stdout, first.stderr) == (0, "task-0\n", "")
    assert (second.returncode, second.stdout, second.stderr) == (0, "task-1\n", "")


def test_array_manifest_safely_preserves_hostile_task_literals(
    tmp_path: PurePosixPath,
) -> None:
    marker = tmp_path / "not-created"
    literal = f"spaces; $(touch {marker}); 'quotes' * and $HOME"
    units = tuple(
        SchedulerUnit(
            TaskId.from_ordinal(index),
            Command(
                ("printf", "%s\n", literal),
                environment={"RUNDRA_LITERAL": literal},
                working_directory=tmp_path,
            ),
            ResourceRequest(),
        )
        for index in range(2)
    )
    request = SlurmArrayRequest(
        SchedulerGroup(units),
        tuple(
            ArrayTaskMapping(unit.task_id, index, index)
            for index, unit in enumerate(units)
        ),
        tmp_path / "tasks.sh",
        2,
    )

    completed = subprocess.run(
        (
            "/bin/sh",
            "-c",
            render_slurm_array_manifest(request),
            "rundra-array-manifest",
            "1",
        ),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 0
    assert completed.stdout == f"{literal}\n"
    assert completed.stderr == ""
    assert not marker.exists()


@pytest.mark.parametrize("arguments", [(), ("-1",), ("2",), ("text",), ("0", "1")])
def test_array_manifest_rejects_missing_or_unknown_indices(
    arguments: tuple[str, ...],
) -> None:
    completed = subprocess.run(
        (
            "/bin/sh",
            "-c",
            render_slurm_array_manifest(_array_request(count=2)),
            "rundra-array-manifest",
            *arguments,
        ),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 64
    assert completed.stdout == ""
    assert completed.stderr == "invalid Rundra array index\n"


def test_array_manifest_rejects_non_array_requests() -> None:
    with pytest.raises(TypeError, match="SlurmArrayRequest"):
        render_slurm_array_manifest(object())  # type: ignore[arg-type]


def test_array_script_renders_bounded_resources_logs_and_manifest_dispatch() -> None:
    resources = ResourceRequest(
        nodes=2,
        tasks=4,
        cpus_per_task=8,
        gpus_per_task=1,
        memory_bytes=16 * 1024**3,
        walltime=timedelta(minutes=5),
        native={"slurm": {"partition": "gpu", "exclusive": True}},
    )
    request = _array_request(count=3, resources=resources, max_array_size=3)

    script = render_sbatch_array_script(
        request,
        stdout_path=PurePosixPath("/remote/logs/%A_%a.stdout"),
        stderr_path=PurePosixPath("/remote/logs/%A_%a.stderr"),
    )

    assert (
        script
        == """\
#!/bin/sh
#SBATCH --job-name=rundra-array
#SBATCH --array=0-2
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=8
#SBATCH --output=/remote/logs/%A_%a.stdout
#SBATCH --error=/remote/logs/%A_%a.stderr
#SBATCH --gpus-per-task=1
#SBATCH --mem=16384M
#SBATCH --time=00:05:00
#SBATCH --partition=gpu
#SBATCH --exclusive

set -eu
if [ "${SLURM_ARRAY_TASK_ID+x}" != x ]; then
  printf '%s\\n' 'missing SLURM_ARRAY_TASK_ID' >&2
  exit 64
fi
exec /bin/sh /remote/run/metadata/tasks.sh "$SLURM_ARRAY_TASK_ID"
"""
    )
    assert "experiment.py" not in script
    assert "--seed" not in script


def test_array_script_executes_selected_manifest_task_with_quoted_path(
    tmp_path: PurePosixPath,
) -> None:
    request = _array_request(
        count=2,
        manifest_path=tmp_path / "manifest with spaces.sh",
        max_array_size=2,
    )
    request.manifest_path.write_text(
        render_slurm_array_manifest(request),
        encoding="utf-8",
    )
    script = render_sbatch_array_script(
        request,
        stdout_path=PurePosixPath("/remote/logs/%A_%a.stdout"),
        stderr_path=PurePosixPath("/remote/logs/%A_%a.stderr"),
    )

    completed = subprocess.run(
        ("/bin/sh", "-c", script),
        env={"SLURM_ARRAY_TASK_ID": "1"},
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 2
    assert "experiment.py" in completed.stderr
    assert "manifest with spaces.sh" in script
    assert "'/tmp/" in script


def test_array_script_rejects_missing_index_at_runtime() -> None:
    script = render_sbatch_array_script(
        _array_request(count=2),
        stdout_path=PurePosixPath("/remote/logs/%A_%a.stdout"),
        stderr_path=PurePosixPath("/remote/logs/%A_%a.stderr"),
    )

    completed = subprocess.run(
        ("/bin/sh", "-c", script),
        env={},
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 64
    assert completed.stderr == "missing SLURM_ARRAY_TASK_ID\n"


@pytest.mark.parametrize(
    ("stdout_path", "stderr_path"),
    [
        (
            PurePosixPath("/remote/logs/%A.stdout"),
            PurePosixPath("/remote/logs/%A_%a.stderr"),
        ),
        (
            PurePosixPath("/remote/logs/%A_%a.stdout"),
            PurePosixPath("/remote/logs/%a.stderr"),
        ),
        (
            PurePosixPath("relative/%A_%a.stdout"),
            PurePosixPath("/remote/logs/%A_%a.stderr"),
        ),
    ],
)
def test_array_script_requires_safe_per_element_log_paths(
    stdout_path: PurePosixPath,
    stderr_path: PurePosixPath,
) -> None:
    with pytest.raises(SlurmScriptError, match="path|%A and %a"):
        render_sbatch_array_script(
            _array_request(count=2),
            stdout_path=stdout_path,
            stderr_path=stderr_path,
        )


def test_array_script_rejects_non_array_requests() -> None:
    with pytest.raises(TypeError, match="SlurmArrayRequest"):
        render_sbatch_array_script(
            object(),  # type: ignore[arg-type]
            stdout_path=PurePosixPath("/remote/logs/%A_%a.stdout"),
            stderr_path=PurePosixPath("/remote/logs/%A_%a.stderr"),
        )


def test_generic_scheduler_submit_still_rejects_ambiguous_multi_task_groups() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport)

    with pytest.raises(SlurmSubmissionError, match="array submission is not available"):
        scheduler.submit(_array_request(count=2).group)

    assert transport.run_calls == []


def test_slurm_scheduler_persists_manifest_and_submits_bounded_array() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        sbatch="/opt/slurm/bin/sbatch",
        log_directory=PurePosixPath("/remote/run/logs"),
    )
    request = _array_request(count=3, max_array_size=3)
    transport.results.append(_command_result(Command(("unused",)), 0, "123;alpha\n"))

    submission = scheduler.submit_bounded_array(request)

    assert submission.reference == SchedulerReference("123")
    assert submission.task_native_ids == {
        TaskId.from_ordinal(0): "123_0",
        TaskId.from_ordinal(1): "123_1",
        TaskId.from_ordinal(2): "123_2",
    }
    command = transport.run_calls[0]
    assert command.argv[0:2] == ("/bin/sh", "-c")
    assert command.argv[4] == str(request.manifest_path)
    encoded_manifest = command.argv[5]
    assert gzip.decompress(base64.b64decode(encoded_manifest)).decode("utf-8") == (
        render_slurm_array_manifest(request)
    )
    assert len(encoded_manifest) < len(render_slurm_array_manifest(request))
    assert command.argv[6] == render_sbatch_array_script(
        request,
        stdout_path=PurePosixPath("/remote/run/logs/%A_%a.stdout"),
        stderr_path=PurePosixPath("/remote/run/logs/%A_%a.stderr"),
    )
    assert command.argv[7:] == ("/remote/run/logs", "/opt/slurm/bin/sbatch")
    assert "already exists" in command.argv[2]
    assert "base64 -d | gzip -d" in command.argv[2]
    assert 'chmod 500 "$manifest_tmp"' in command.argv[2]


def test_large_array_manifest_uses_a_bounded_compressed_argument() -> None:
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), 0, "126\n")])
    )
    scheduler = SlurmScheduler(
        transport,
        log_directory=PurePosixPath("/remote/run/logs"),
    )
    request = _array_request(count=200, max_array_size=200)

    submission = scheduler.submit_bounded_array(request)

    encoded_manifest = transport.run_calls[0].argv[5]
    decoded_manifest = gzip.decompress(base64.b64decode(encoded_manifest)).decode(
        "utf-8"
    )
    assert submission.reference == SchedulerReference("126")
    assert decoded_manifest == render_slurm_array_manifest(request)
    assert len(encoded_manifest) < 128 * 1024


def test_slurm_single_submission_uses_framework_owned_afterok_dependency() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        sbatch="/opt/slurm/bin/sbatch",
        log_directory=PurePosixPath("/remote/run/logs"),
    )
    transport.results.append(_command_result(Command(("unused",)), 0, "124\n"))

    submission = scheduler.submit_afterok(_group(), SchedulerReference("123"))

    assert submission.reference == SchedulerReference("124")
    command = transport.run_calls[0]
    assert command.argv[-1] == "afterok:123"
    assert '--dependency="$4"' in command.argv[2]
    assert "#SBATCH --dependency" not in command.argv[4]


def test_slurm_array_submission_uses_framework_owned_afterok_dependency() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        log_directory=PurePosixPath("/remote/run/logs"),
    )
    request = _array_request(count=2)
    portable = SchedulerArrayRequest(
        request.group,
        request.mapping,
        request.manifest_path,
    )
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 1001\n"),
            _command_result(Command(("unused",)), 0, "125\n"),
        )
    )

    submission = scheduler.submit_array_afterok(
        portable,
        SchedulerReference("123"),
    )

    assert submission.reference == SchedulerReference("125")
    command = transport.run_calls[1]
    assert command.argv[-1] == "afterok:123"
    assert '--dependency="$6"' in command.argv[2]
    assert "#SBATCH --dependency" not in command.argv[6]


@pytest.mark.parametrize("native_id", ["123_0", "afterok:123", "abc"])
def test_slurm_afterok_rejects_non_root_job_ids(native_id: str) -> None:
    scheduler = SlurmScheduler(ScriptedTransport(deque([])))

    with pytest.raises(SlurmSubmissionError, match="root numeric job ID"):
        scheduler.submit_afterok(_group(), SchedulerReference(native_id))


def test_slurm_array_submission_requires_durable_log_paths() -> None:
    transport = ScriptedTransport(deque([]))

    with pytest.raises(SlurmSubmissionError, match="configured log directory"):
        SlurmScheduler(transport).submit_bounded_array(_array_request(count=2))

    assert transport.run_calls == []


@pytest.mark.parametrize(
    ("exit_code", "stdout", "stderr", "message"),
    [
        (73, "", "manifest exists", "exit code 73"),
        (0, "Submitted batch job 123", "", "invalid parsable"),
    ],
)
def test_slurm_array_submission_normalizes_remote_failures(
    exit_code: int, stdout: str, stderr: str, message: str
) -> None:
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), exit_code, stdout, stderr)])
    )
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )

    with pytest.raises(SlurmSubmissionError, match=message):
        scheduler.submit_bounded_array(_array_request(count=2))


def test_slurm_scheduler_discovers_bound_for_portable_array_request() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )
    bounded = _array_request(count=2)
    portable = SchedulerArrayRequest(
        bounded.group, bounded.mapping, bounded.manifest_path
    )
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 1001\n"),
            _command_result(Command(("unused",)), 0, "456\n"),
        )
    )

    submission = scheduler.submit_array(portable)

    assert submission.reference == SchedulerReference("456")
    assert submission.task_native_ids == {
        TaskId.from_ordinal(0): "456_0",
        TaskId.from_ordinal(1): "456_1",
    }


def test_slurm_scheduler_splits_portable_arrays_at_controller_bound() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )
    source = _array_request(count=5)
    portable = SchedulerArrayRequest(source.group, source.mapping, source.manifest_path)
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 2\n"),
            _command_result(Command(("unused",)), 0, "501\n"),
            _command_result(Command(("unused",)), 0, "502\n"),
            _command_result(Command(("unused",)), 0, "503\n"),
        )
    )

    submission = scheduler.submit_array(portable)

    assert submission.references == (
        SchedulerReference("501"),
        SchedulerReference("502"),
        SchedulerReference("503"),
    )
    assert submission.task_native_ids == {
        TaskId.from_ordinal(0): "501_0",
        TaskId.from_ordinal(1): "501_1",
        TaskId.from_ordinal(2): "502_0",
        TaskId.from_ordinal(3): "502_1",
        TaskId.from_ordinal(4): "503",
    }
    assert [call.argv[4] for call in transport.run_calls[1:3]] == [
        "/remote/run/metadata/tasks.part-000000.sh",
        "/remote/run/metadata/tasks.part-000001.sh",
    ]
    assert transport.run_calls[3].argv[0:2] == ("/bin/sh", "-c")


def test_slurm_scheduler_cancels_submitted_roots_after_partial_chunk_failure() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )
    source = _array_request(count=3)
    portable = SchedulerArrayRequest(source.group, source.mapping, source.manifest_path)
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 2\n"),
            _command_result(Command(("unused",)), 0, "601\n"),
            _command_result(Command(("unused",)), 1, "", "rejected"),
            _command_result(Command(("unused",)), 0, ""),
        )
    )

    with pytest.raises(SlurmSubmissionError, match="exit code 1"):
        scheduler.submit_array(portable)

    assert transport.run_calls[-1] == Command(("scancel", "--", "601"))


def test_slurm_scheduler_bundles_tasks_at_concurrent_job_limit() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )
    source = _array_request(
        count=5,
        resources=ResourceRequest(walltime=timedelta(minutes=2)),
    )
    portable = SchedulerArrayRequest(
        source.group,
        source.mapping,
        source.manifest_path,
        max_concurrent_jobs=2,
    )
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 1001\n"),
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, "777\n"),
        )
    )

    submission = scheduler.submit_array(portable)

    assert submission.references == (SchedulerReference("777"),)
    assert submission.task_native_ids == {
        TaskId.from_ordinal(0): "777_0",
        TaskId.from_ordinal(1): "777_1",
        TaskId.from_ordinal(2): "777_0",
        TaskId.from_ordinal(3): "777_1",
        TaskId.from_ordinal(4): "777_0",
    }
    command = transport.run_calls[-1]
    encoded = "".join(call.argv[4] for call in transport.run_calls[2:-1])
    manifest = gzip.decompress(base64.b64decode(encoded)).decode("utf-8")
    assert "bundle-status" in manifest
    assert "timeout --signal=TERM --kill-after=30s 120s env --" in manifest
    assert "120s exec env --" not in manifest
    assert "exec timeout" not in manifest
    assert manifest.count("# task_id=") == 5
    assert "#SBATCH --array=0-1" in command.argv[6]
    assert "#SBATCH --time=00:06:00" in command.argv[6]


def test_slurm_scheduler_runs_concurrent_lanes_in_bounded_workers() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport, log_directory=PurePosixPath("/remote/run/logs")
    )
    source = _array_request(
        count=8,
        resources=ResourceRequest(
            cpus_per_task=1,
            memory_bytes=1024**3,
            walltime=timedelta(minutes=2),
        ),
    )
    portable = SchedulerArrayRequest(
        source.group,
        source.mapping,
        source.manifest_path,
        max_concurrent_jobs=8,
        max_workers=2,
        task_slots_per_worker=2,
        output_root=PurePosixPath("/remote/run/output"),
        shard_root=PurePosixPath("/remote/run/output/.rundra-shards"),
    )
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, "MaxArraySize = 1001\n"),
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, "888\n"),
        )
    )

    submission = scheduler.submit_array(portable)

    assert submission.task_native_ids == {
        TaskId.from_ordinal(index): f"888_{index % 2}" for index in range(8)
    }
    command = transport.run_calls[-1]
    encoded = "".join(call.argv[4] for call in transport.run_calls[2:-1])
    manifest = gzip.decompress(base64.b64decode(encoded)).decode("utf-8")
    script = command.argv[6]
    assert manifest.count("# task_id=") == 8
    assert 'case "$SLURM_PROCID" in' in manifest
    assert ".lane-${SLURM_PROCID}.tsv" in manifest
    assert "RUNDRA_SHARD\\t2" in manifest
    assert "sha256sum" in manifest
    assert ".rundra-shards" in manifest
    assert "tar --sort=name" in manifest
    assert "#SBATCH --array=0-1" in script
    assert "#SBATCH --ntasks=2" in script
    assert "#SBATCH --mem=2048M" in script
    assert "#SBATCH --time=00:04:00" in script
    assert "srun --nodes=1 --ntasks=2 --ntasks-per-node=2" in script
    assert script.count("srun ") == 1


def test_render_sbatch_script_translates_portable_and_allowed_native_resources() -> (
    None
):
    script = render_sbatch_script(
        _group(
            resources=ResourceRequest(
                nodes=2,
                tasks=4,
                cpus_per_task=8,
                gpus_per_task=1,
                memory_bytes=16 * 1024**3,
                walltime=timedelta(days=1, hours=2, minutes=3, seconds=4),
                native={
                    "slurm": {
                        "partition": "gpu",
                        "account": "science",
                        "qos": "normal",
                        "constraint": "a100",
                        "exclusive": True,
                    }
                },
            )
        )
    )

    assert (
        script
        == """\
#!/bin/sh
#SBATCH --job-name=rundra-task_000000
#SBATCH --nodes=2
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=8
#SBATCH --gpus-per-task=1
#SBATCH --mem=16384M
#SBATCH --time=1-02:03:04
#SBATCH --account=science
#SBATCH --constraint=a100
#SBATCH --partition=gpu
#SBATCH --qos=normal
#SBATCH --exclusive

set -eu
exec env -- python3 experiment.py
"""
    )


def test_rendered_script_preserves_hostile_command_literals_without_execution(
    tmp_path: PurePosixPath,
) -> None:
    marker = tmp_path / "not-created"
    literal = f"spaces; $(touch {marker}); 'quotes' and *"
    script = render_sbatch_script(
        _group(
            command=Command(
                ("printf", "%s\\n", literal),
                environment={"LITERAL": literal},
                working_directory=tmp_path,
            )
        )
    )

    completed = subprocess.run(
        ("/bin/sh", "-c", script),
        capture_output=True,
        check=False,
        encoding="utf-8",
        shell=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert completed.stdout == f"{literal}\n"
    assert not marker.exists()


def test_resource_rounding_never_requests_less_than_the_portable_value() -> None:
    script = render_sbatch_script(
        _group(
            resources=ResourceRequest(
                memory_bytes=1024**2 + 1,
                walltime=timedelta(seconds=1, microseconds=1),
                native={"slurm": {"exclusive": False}},
            )
        )
    )

    assert "#SBATCH --mem=2M" in script
    assert "#SBATCH --time=00:00:02" in script
    assert "--exclusive" not in script


def test_sbatch_script_uses_explicit_normalized_log_paths() -> None:
    script = render_sbatch_script(
        _group(),
        stdout_path=PurePosixPath("/remote/logs/%j.stdout"),
        stderr_path=PurePosixPath("/remote/logs/%j.stderr"),
    )

    assert "#SBATCH --output=/remote/logs/%j.stdout" in script
    assert "#SBATCH --error=/remote/logs/%j.stderr" in script


@pytest.mark.parametrize(
    ("stdout", "stderr"),
    [
        (PurePosixPath("relative.out"), PurePosixPath("/logs/error")),
        (PurePosixPath("/logs/with space"), PurePosixPath("/logs/error")),
        (PurePosixPath("/logs/output"), None),
    ],
)
def test_sbatch_log_paths_must_be_complete_absolute_and_directive_safe(
    stdout: PurePosixPath, stderr: PurePosixPath | None
) -> None:
    with pytest.raises(SlurmScriptError, match="path|provided together"):
        render_sbatch_script(_group(), stdout_path=stdout, stderr_path=stderr)


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"nodes": 4}, "Unsupported"),
        ({"output": "stolen"}, "Unsupported"),
        ({"reservation": "special"}, "Unsupported"),
        ({"partition": "gpu\n#SBATCH --nodes=99"}, "unsafe"),
        ({"partition": "gpu --nodes=99"}, "unsafe"),
        ({"partition": "gpu;danger"}, "unsafe"),
        ({"partition": "$(danger)"}, "unsafe"),
        ({"partition": "#comment"}, "unsafe"),
        ({"qos": True}, "string or integer"),
        ({"exclusive": "yes"}, "boolean"),
    ],
)
def test_native_slurm_options_are_explicitly_allowlisted(
    options: dict[str, object], message: str
) -> None:
    resources = ResourceRequest(native={"slurm": options})  # type: ignore[arg-type]

    with pytest.raises(SlurmScriptError, match=message):
        render_sbatch_script(_group(resources=resources))


def test_slurm_rejects_native_options_for_a_different_scheduler() -> None:
    resources = ResourceRequest(native={"pbs": {"queue": "batch"}})

    with pytest.raises(SlurmScriptError, match="backend namespaces.*pbs"):
        render_sbatch_script(_group(resources=resources))


def test_m3_renderer_rejects_multi_task_groups_and_non_groups() -> None:
    first = _group().units[0]
    second = _group(ordinal=1).units[0]

    with pytest.raises(SlurmScriptError, match="exactly one Task"):
        render_sbatch_script(SchedulerGroup((first, second)))
    with pytest.raises(TypeError, match="SchedulerGroup"):
        render_sbatch_script(object())  # type: ignore[arg-type]


def _command_result(
    command: Command, exit_code: int, stdout: str, stderr: str = ""
) -> CommandResult:
    now = datetime(2026, 8, 15, tzinfo=UTC)
    return CommandResult(command, exit_code, stdout, stderr, now, now)


def test_slurm_scheduler_submits_generated_script_with_parsable_output() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport, sbatch="/opt/slurm/bin/sbatch")
    group = _group()
    expected_script = render_sbatch_script(group)
    expected_command = Command(
        (
            "/bin/sh",
            "-c",
            "set -eu\n"
            'script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")\n'
            "trap 'rm -f \"$script\"' EXIT HUP INT TERM\n"
            'printf \'%s\' "$1" > "$script"\n'
            '"$2" --parsable "$script"\n',
            "rundra-slurm-submit",
            expected_script,
            "/opt/slurm/bin/sbatch",
        )
    )
    transport.results.append(_command_result(expected_command, 0, "12345;alpha\n"))

    submission = scheduler.submit(group)

    assert isinstance(scheduler, Scheduler)
    assert submission.reference.native_id == "12345"
    assert submission.task_native_ids == {group.units[0].task_id: "12345"}
    assert transport.run_calls == [expected_command]


def test_slurm_submission_creates_and_uses_configured_log_directory() -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/work/.scheduler-logs"),
    )
    transport.results.append(_command_result(Command(("unused",)), 0, "12345\n"))

    scheduler.submit(_group())

    command = transport.run_calls[0]
    assert "mkdir -p" in command.argv[2]
    assert command.argv[-1] == "/remote/work/.scheduler-logs"
    assert "#SBATCH --output=/remote/work/.scheduler-logs/%j.stdout" in command.argv[4]
    assert "#SBATCH --error=/remote/work/.scheduler-logs/%j.stderr" in command.argv[4]


@pytest.mark.parametrize(
    ("exit_code", "stdout", "stderr", "message"),
    [
        (1, "", "invalid account", "exit code 1"),
        (0, "Submitted batch job 123", "", "invalid parsable"),
        (0, "123\nextra", "", "invalid parsable"),
        (0, "123;cluster;extra", "", "invalid parsable"),
    ],
)
def test_slurm_submission_rejects_failures_and_nonparsable_output(
    exit_code: int, stdout: str, stderr: str, message: str
) -> None:
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport)
    group = _group()
    expected = Command(
        (
            "/bin/sh",
            "-c",
            "set -eu\n"
            'script=$(mktemp "${TMPDIR:-/tmp}/rundra-sbatch.XXXXXX")\n'
            "trap 'rm -f \"$script\"' EXIT HUP INT TERM\n"
            'printf \'%s\' "$1" > "$script"\n'
            '"$2" --parsable "$script"\n',
            "rundra-slurm-submit",
            render_sbatch_script(group),
            "sbatch",
        )
    )
    transport.results.append(_command_result(expected, exit_code, stdout, stderr))

    with pytest.raises(SlurmSubmissionError, match=message):
        scheduler.submit(group)


def test_slurm_submission_normalizes_transport_start_failure() -> None:
    transport = ScriptedTransport(deque([RuntimeError("connection lost")]))

    with pytest.raises(SlurmSubmissionError, match="Could not start"):
        SlurmScheduler(transport).submit(_group())


def test_slurm_query_combines_queue_and_accounting_in_request_order() -> None:
    first = SchedulerReference("123")
    second = SchedulerReference("456")
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/work/.scheduler-logs"),
    )
    squeue_command = Command(
        (
            "squeue",
            "--noheader",
            "--array",
            "--jobs",
            "123,456",
            "--format",
            "%i|%T|%S|%N",
        )
    )
    sacct_command = Command(
        (
            "sacct",
            "--noheader",
            "--parsable2",
            "--jobs",
            "456",
            "--format",
            "JobID,State%32,ExitCode,Start,End,NodeList",
        )
    )
    transport.results.extend(
        (
            _command_result(
                squeue_command,
                0,
                "123|RUNNING|2026-08-15T10:01:02|node[01-02]\n",
            ),
            _command_result(
                sacct_command,
                0,
                "456|OUT_OF_MEMORY|137:0|2026-08-15T09:00:00|"
                "2026-08-15T09:01:00|node03|\n"
                "456.batch|FAILED|137:0|2026-08-15T09:00:00|"
                "2026-08-15T09:01:00|node03|\n",
            ),
        )
    )

    observations = scheduler.query((first, second))

    assert [item.reference for item in observations] == [first, second]
    assert observations[0].state is ExecutionState.RUNNING
    assert observations[0].native_state == "RUNNING"
    assert observations[0].started_at == datetime(2026, 8, 15, 10, 1, 2, tzinfo=UTC)
    assert observations[0].metadata == {
        "source": "squeue",
        "allocated_nodes": "node[01-02]",
        "native_start": "2026-08-15T10:01:02",
        "stdout_path": "/remote/work/.scheduler-logs/123.stdout",
        "stderr_path": "/remote/work/.scheduler-logs/123.stderr",
    }
    assert observations[1].state is ExecutionState.FAILED
    assert observations[1].native_state == "OUT_OF_MEMORY"
    assert observations[1].exit_code == 137
    assert observations[1].finished_at == datetime(2026, 8, 15, 9, 1, tzinfo=UTC)
    assert transport.run_calls == [squeue_command, sacct_command]


def test_slurm_query_reconciles_array_elements_independently() -> None:
    first = SchedulerReference("123_0")
    second = SchedulerReference("123_1")
    third = SchedulerReference("123_2")
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(
        transport,
        timezone=UTC,
        log_directory=PurePosixPath("/remote/run/logs"),
    )
    transport.results.extend(
        (
            _command_result(
                Command(("unused",)),
                0,
                "123_1|RUNNING|2026-08-15T10:02:00|node02\n",
            ),
            _command_result(
                Command(("unused",)),
                0,
                "123_0|COMPLETED|0:0|2026-08-15T10:00:00|"
                "2026-08-15T10:01:00|node01|\n"
                "123_0.batch|COMPLETED|0:0|2026-08-15T10:00:00|"
                "2026-08-15T10:01:00|node01|\n"
                "123_2|FAILED|9:0|2026-08-15T10:00:00|"
                "2026-08-15T10:01:30|node03|\n",
            ),
        )
    )

    observations = scheduler.query((first, second, third))

    assert [observation.reference for observation in observations] == [
        first,
        second,
        third,
    ]
    assert [observation.state for observation in observations] == [
        ExecutionState.SUCCEEDED,
        ExecutionState.RUNNING,
        ExecutionState.FAILED,
    ]
    assert [observation.exit_code for observation in observations] == [0, None, 9]
    assert observations[0].metadata["stdout_path"] == "/remote/run/logs/123_0.stdout"
    assert observations[1].metadata["stderr_path"] == "/remote/run/logs/123_1.stderr"
    assert observations[2].metadata["stdout_path"] == "/remote/run/logs/123_2.stdout"


def test_slurm_query_batches_large_reference_sets() -> None:
    references = tuple(SchedulerReference(str(index)) for index in range(1, 502))
    first, second = references[:500], references[500:]
    transport = ScriptedTransport(
        deque(
            (
                _command_result(
                    Command(("unused",)),
                    0,
                    "".join(
                        f"{item.native_id}|RUNNING|Unknown|node01\n" for item in first
                    ),
                ),
                _command_result(
                    Command(("unused",)),
                    0,
                    "".join(
                        f"{item.native_id}|RUNNING|Unknown|node01\n" for item in second
                    ),
                ),
            )
        )
    )

    observations = SlurmScheduler(transport).query(references)

    assert tuple(item.reference for item in observations) == references
    assert len(transport.run_calls) == 2
    assert transport.run_calls[0].argv[4].count(",") == 499
    assert transport.run_calls[1].argv[4] == "501"


def test_slurm_controller_array_limit_is_explicitly_discovered() -> None:
    transport = ScriptedTransport(
        deque(
            [
                _command_result(
                    Command(("scontrol", "show", "config")),
                    0,
                    "ClusterName = shoal\nMaxArraySize = 1001\nSlurmctldPort = 6817\n",
                )
            ]
        )
    )

    assert SlurmScheduler(transport).array_limit() == 1001


@pytest.mark.parametrize("output", ["ClusterName = shoal\n", "MaxArraySize = 0\n"])
def test_slurm_controller_rejects_missing_or_invalid_array_limit(output: str) -> None:
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), 0, output)])
    )

    with pytest.raises(SlurmQueryError, match="MaxArraySize"):
        SlurmScheduler(transport).array_limit()


@pytest.mark.parametrize("native_id", ["123_", "123_1.batch", "123-1", "text"])
def test_slurm_query_rejects_non_element_scheduler_references(native_id: str) -> None:
    scheduler = SlurmScheduler(ScriptedTransport(deque([])))

    with pytest.raises(ValueError, match="job or job_array-index"):
        scheduler.query((SchedulerReference(native_id),))


def test_naive_slurm_timestamps_are_not_fabricated_without_site_timezone() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|RUNNING|2026-08-15T10:01:02|node01\n",
                )
            ]
        )
    )

    observation = SlurmScheduler(transport).query((reference,))[0]

    assert observation.started_at is None
    assert observation.metadata["native_start"] == "2026-08-15T10:01:02"


def test_slurm_query_marks_accounting_lag_explicitly() -> None:
    reference = SchedulerReference("789")
    transport = ScriptedTransport(deque([]))
    scheduler = SlurmScheduler(transport)
    transport.results.extend(
        (
            _command_result(Command(("unused",)), 0, ""),
            _command_result(Command(("unused",)), 0, ""),
        )
    )

    observation = scheduler.query((reference,))[0]

    assert observation.state is ExecutionState.UNKNOWN
    assert observation.native_state == "ACCOUNTING_PENDING"
    assert observation.metadata == {"accounting_pending": True}


def test_slurm_query_falls_back_to_scontrol_when_accounting_is_disabled() -> None:
    reference = SchedulerReference("18")
    transport = ScriptedTransport(
        deque(
            (
                _command_result(Command(("unused",)), 0, ""),
                _command_result(
                    Command(("unused",)),
                    1,
                    "",
                    "Slurm accounting storage is disabled",
                ),
                _command_result(
                    Command(("unused",)),
                    0,
                    "JobId=18 JobState=COMPLETED ExitCode=0:0 "
                    "StartTime=2026-08-15T21:13:48 "
                    "EndTime=2026-08-15T21:13:49 NodeList=shoal1\n",
                ),
            )
        )
    )

    observation = SlurmScheduler(transport).query((reference,))[0]

    assert observation.state is ExecutionState.SUCCEEDED
    assert observation.native_state == "COMPLETED"
    assert observation.exit_code == 0
    assert observation.started_at is None
    assert observation.finished_at is None
    assert observation.metadata == {
        "source": "scontrol",
        "native_start": "2026-08-15T21:13:48",
        "native_end": "2026-08-15T21:13:49",
        "allocated_nodes": "shoal1",
    }
    assert transport.run_calls[-1] == Command(("scontrol", "show", "job", "-o", "18"))


def test_slurm_query_reports_failed_scontrol_fallback() -> None:
    transport = ScriptedTransport(
        deque(
            (
                _command_result(Command(("unused",)), 0, ""),
                _command_result(Command(("unused",)), 1, "", "accounting disabled"),
                _command_result(Command(("unused",)), 1, "", "invalid job id"),
            )
        )
    )

    with pytest.raises(SlurmQueryError, match="scontrol fallback failed"):
        SlurmScheduler(transport).query((SchedulerReference("18"),))


@pytest.mark.parametrize(
    ("native", "exit_code", "expected"),
    [
        ("PENDING", None, ExecutionState.QUEUED),
        ("CONFIGURING", None, ExecutionState.QUEUED),
        ("RUNNING", None, ExecutionState.RUNNING),
        ("COMPLETING", None, ExecutionState.RUNNING),
        ("COMPLETED", 0, ExecutionState.SUCCEEDED),
        ("COMPLETED", 1, ExecutionState.FAILED),
        ("FAILED", 1, ExecutionState.FAILED),
        ("OUT_OF_MEMORY", 137, ExecutionState.FAILED),
        ("TIMEOUT", 0, ExecutionState.FAILED),
        ("PREEMPTED", 0, ExecutionState.FAILED),
        ("CANCELLED by 42", 0, ExecutionState.CANCELLED),
        ("FUTURE_STATE", None, ExecutionState.UNKNOWN),
    ],
)
def test_slurm_native_states_have_comprehensive_portable_mapping(
    native: str, exit_code: int | None, expected: ExecutionState
) -> None:
    assert _portable_state(native, exit_code) is expected


@pytest.mark.parametrize(
    ("results", "message"),
    [
        ((1, "", "permission denied"), "squeue failed"),
        ((0, "malformed", ""), "malformed row"),
    ],
)
def test_slurm_query_rejects_command_and_parse_failures(
    results: tuple[int, str, str], message: str
) -> None:
    exit_code, stdout, stderr = results
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), exit_code, stdout, stderr)])
    )

    with pytest.raises(SlurmQueryError, match=message):
        SlurmScheduler(transport).query((SchedulerReference("123"),))


def test_slurm_query_validates_reference_collection() -> None:
    scheduler = SlurmScheduler(ScriptedTransport(deque([])))
    reference = SchedulerReference("123")

    assert scheduler.query(()) == ()
    with pytest.raises(ValueError, match="unique"):
        scheduler.query((reference, reference))
    with pytest.raises(ValueError, match="numeric"):
        scheduler.query((SchedulerReference("local-1"),))


def test_slurm_cancel_requests_then_reconciles_state() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 0, ""),
                _command_result(Command(("unused",)), 0, "123|CANCELLED|N/A|(null)\n"),
            ]
        )
    )

    observation = SlurmScheduler(transport).cancel((reference,))[0]

    assert observation.state is ExecutionState.CANCELLED
    assert transport.run_calls[0] == Command(("scancel", "--", "123"))


def test_slurm_cancel_treats_terminal_invalid_job_race_as_success() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 1, "", "invalid job id"),
                _command_result(Command(("unused",)), 0, ""),
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|COMPLETED|0:0|2026-08-15T10:00:00|"
                    "2026-08-15T10:01:00|node01|\n",
                ),
            ]
        )
    )

    observation = SlurmScheduler(transport).cancel((reference,))[0]

    assert observation.state is ExecutionState.SUCCEEDED


def test_slurm_cancel_reports_nonzero_when_job_remains_active() -> None:
    reference = SchedulerReference("123")
    transport = ScriptedTransport(
        deque(
            [
                _command_result(Command(("unused",)), 1, "", "permission denied"),
                _command_result(
                    Command(("unused",)),
                    0,
                    "123|RUNNING|2026-08-15T10:00:00|node01\n",
                ),
            ]
        )
    )

    with pytest.raises(SlurmCancellationError) as caught:
        SlurmScheduler(transport).cancel((reference,))
    assert "permission denied" not in str(caught.value)
    assert "diagnostic redacted" in str(caught.value)


def test_slurm_errors_do_not_expose_scheduler_stderr() -> None:
    secret = "API_TOKEN=must-not-leak"
    transport = ScriptedTransport(
        deque([_command_result(Command(("unused",)), 1, "", secret)])
    )

    with pytest.raises(SlurmSubmissionError) as caught:
        SlurmScheduler(transport).submit(_group())

    assert secret not in str(caught.value)
    assert "exit code 1" in str(caught.value)
    assert "diagnostic redacted" in str(caught.value)
