from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import timedelta
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePath

from rundra.adapters.local import LocalTransport
from rundra.adapters.ssh import SSHTransport
from rundra.cli.doctor import DoctorCheck
from rundra.cli.doctor import doctor_operation as target_doctor_operation
from rundra.config.errors import ConfigError
from rundra.config.experiments import load_experiment
from rundra.config.targets import TargetsConfig, load_targets_config
from rundra.domain.models import (
    Command,
    ResourceRequest,
    Target,
    TaskId,
)
from rundra.domain.preparation import PreparationPlan, PreparationStorageConfig
from rundra.domain.states import ExecutionState
from rundra.orchestration.preparation import (
    probe_local_offline_preparation,
    probe_remote_offline_preparation,
    select_remote_preparation_location,
)
from rundra.ports import Scheduler, SchedulerGroup, SchedulerReference, SchedulerUnit
from rundra.results import OperationError, OperationResult
from rundra.scheduler_registry import scheduler_for_target

_TERMINAL = frozenset(
    {ExecutionState.SUCCEEDED, ExecutionState.FAILED, ExecutionState.CANCELLED}
)


@dataclass(frozen=True, slots=True)
class DoctorRequirement:
    kind: str
    value: str
    access: str
    purpose: str
    location: str
    status: str


@dataclass(frozen=True, slots=True)
class DoctorAction:
    code: str
    message: str


@dataclass(frozen=True, slots=True)
class DoctorValue:
    source: Path
    target: Target | None
    mode: str
    checks: tuple[DoctorCheck, ...]
    requirements: tuple[DoctorRequirement, ...]
    actions: tuple[DoctorAction, ...]
    connected: bool
    scheduler_probed: bool
    agent: str
    agent_config: str | None
    format_version: int = 3

    @property
    def ready(self) -> bool:
        return all(check.status != "fail" for check in self.checks)

    @property
    def complete(self) -> bool:
        return self.ready and all(check.status == "pass" for check in self.checks)


def doctor_operation(
    targets_file: Path,
    target_name: str | None,
    *,
    connect: bool = False,
    scheduler_probe: bool = False,
    probe_timeout: int = 120,
    write_probe: bool = True,
    data_dir: Path | None = None,
    destination: Path | None = None,
    source_root: Path | None = None,
    experiment_source: Path | None = None,
    config_source: Path | None = None,
    cache_root: Path | None = None,
    preparation: PreparationPlan | None = None,
    preparation_storage: PreparationStorageConfig | None = None,
    offline: bool = False,
    local_target_access: bool = False,
    agent: str = "generic",
) -> OperationResult[DoctorValue]:
    """Audit effective client, target, staging, and optional scheduler access."""
    if probe_timeout < 1 or probe_timeout > 600:
        return _usage("--probe-timeout must be from 1 to 600")
    if scheduler_probe and not write_probe:
        return _usage("--scheduler-probe requires local write probes")
    if local_target_access and target_name is None:
        return _usage("--local-target-access requires a selected target")
    if offline and experiment_source is None:
        return _usage("--offline requires an experiment")
    connect = connect or scheduler_probe
    preparation_storage = preparation_storage or PreparationStorageConfig()
    source = targets_file.expanduser().resolve()
    mode = (
        "experiment" if experiment_source else "target" if target_name else "bootstrap"
    )
    checks = [_package_check()]
    requirements: list[DoctorRequirement] = []

    target_config = _file_check(source, "targets_config")
    checks.append(target_config)
    requirements.append(
        _path_requirement(source, "read", "target definitions", target_config)
    )
    targets_config: TargetsConfig | None = None
    if target_config.status == "pass":
        try:
            targets_config = load_targets_config(source)
            checks.append(
                DoctorCheck("target_config", "pass", "target configuration is valid")
            )
        except ConfigError as error:
            checks.append(DoctorCheck("target_config", "fail", error.message))

    user_config = Path("~/.config/rundra/config.yaml").expanduser().resolve()
    if user_config.exists():
        check = _file_check(user_config, "user_config")
        checks.append(check)
        requirements.append(
            _path_requirement(user_config, "read", "user defaults", check)
        )

    for name, path, purpose in (
        ("run_store", data_dir or Path("~/.local/share/rundra/runs"), "persist Runs"),
        (
            "preparation_cache",
            cache_root or Path("~/.cache/rundra"),
            "cache preparation",
        ),
    ):
        check = _directory_check(path, name, write_probe)
        checks.append(check)
        requirements.append(_path_requirement(path, "write", purpose, check))

    for name, candidate_path, purpose in (
        ("experiment", experiment_source, "read the experiment"),
        ("config", config_source, "read the effective config"),
        ("source_root", source_root, "snapshot source inputs"),
    ):
        if candidate_path is not None:
            check = _read_path_check(candidate_path, name)
            checks.append(check)
            requirements.append(
                _path_requirement(candidate_path, "read", purpose, check)
            )
    if destination is not None:
        check = _directory_check(destination, "destination", write_probe)
        checks.append(check)
        requirements.append(
            _path_requirement(destination, "write", "retrieve outputs", check)
        )
    checks.append(_guide_check(Path.cwd() / "AGENTS.md"))

    target: Target | None = None
    connected = False
    if target_name:
        legacy = target_doctor_operation(source, target_name, connect=connect)
        if legacy.ok and legacy.value is not None:
            target = legacy.value.target
            assert targets_config is not None
            checks.extend(legacy.value.checks)
            connected = connect and any(
                item.name == "connect" and item.status == "pass"
                for item in legacy.value.checks
            )
            target_requirements = _target_requirements(target, connected)
            requirements.extend(target_requirements)
            if local_target_access or target.staging.kind == "shared":
                local_checks, local_requirements = _local_target_access_checks(
                    target,
                    targets_config.preparation.get(
                        target_name, PreparationStorageConfig()
                    ),
                    write_probe,
                )
                checks.extend(local_checks)
                requirements.extend(local_requirements)
            if target.transport.kind == "local":
                workspace = _directory_check(
                    Path(target.workspace), "workspace", write_probe
                )
                checks.append(workspace)
                requirements.append(
                    _path_requirement(
                        Path(target.workspace), "write", "stage local Runs", workspace
                    )
                )
                connected = True
            elif connected:
                staging = _staging_roundtrip(target)
                checks.append(staging)
                requirements[-1] = _remote_workspace_requirement(target, staging)
            elif not connect:
                checks.append(
                    DoctorCheck(
                        "remote_access",
                        "warning",
                        "remote access was not exercised; rerun with --connect",
                    )
                )
            if scheduler_probe and connected:
                checks.append(_scheduler_probe(target, probe_timeout))
            if offline and preparation is not None and experiment_source is not None:
                location = (
                    "local"
                    if target.transport.kind == "local"
                    else select_remote_preparation_location(
                        preparation, preparation_storage.definition_build
                    )
                )
                use_local = location == "local"
                if use_local:
                    try:
                        experiment = load_experiment(experiment_source)
                        local_cache = (
                            Path(preparation_storage.cache_root)
                            if preparation_storage.cache_root is not None
                            else (cache_root or Path("~/.cache/rundra")).expanduser()
                        )
                        probe = probe_local_offline_preparation(
                            preparation,
                            experiment,
                            target,
                            source_root=source_root or experiment_source.parent,
                            cache_root=local_cache.resolve(),
                            image_search_paths=preparation_storage.image_search_paths,
                            definition_build=preparation_storage.definition_build,
                        )
                        checks.extend(
                            (
                                DoctorCheck(
                                    "offline_source_cache",
                                    "pass" if probe.source_ready else "fail",
                                    probe.source_message,
                                ),
                                DoctorCheck(
                                    "offline_image_cache",
                                    (
                                        "warning"
                                        if probe.image_ready is None
                                        else "pass"
                                        if probe.image_ready
                                        else "fail"
                                    ),
                                    probe.image_message,
                                ),
                            )
                        )
                    except ConfigError as error:
                        checks.append(
                            DoctorCheck(
                                "offline_preparation_cache", "fail", error.message
                            )
                        )
                elif connected:
                    target_storage = targets_config.preparation.get(
                        target_name, PreparationStorageConfig()
                    )
                    probe = probe_remote_offline_preparation(
                        preparation,
                        target,
                        _transport(target),
                        cache_root=target_storage.cache_root,
                        image_search_paths=target_storage.image_search_paths,
                    )
                    checks.extend(
                        (
                            DoctorCheck(
                                "offline_source_cache",
                                "pass" if probe.source_ready else "fail",
                                probe.source_message,
                            ),
                            DoctorCheck(
                                "offline_image_cache",
                                (
                                    "warning"
                                    if probe.image_ready is None
                                    else "pass"
                                    if probe.image_ready
                                    else "fail"
                                ),
                                probe.image_message,
                            ),
                        )
                    )
                else:
                    checks.append(
                        DoctorCheck(
                            "offline_target_cache",
                            "fail",
                            "target cache was not checked; rerun with --connect",
                        )
                    )
        else:
            assert legacy.error is not None
            checks.append(DoctorCheck("selected_target", "fail", legacy.error.message))
    elif mode != "bootstrap":
        checks.append(DoctorCheck("selected_target", "fail", "no target was selected"))

    actions = list(_actions(checks, connected, scheduler_probe))
    statuses = {check.name: check.status for check in checks}
    if statuses.get("offline_source_cache") == "fail":
        actions.append(
            DoctorAction(
                "OFFLINE_SOURCE_CACHE_MISS",
                "rerun the preparation once without --offline to cache the pinned Git source",
            )
        )
    if statuses.get("offline_image_cache") == "fail":
        actions.append(
            DoctorAction(
                "OFFLINE_IMAGE_CACHE_MISS",
                "rerun the preparation once without --offline to cache the verified image",
            )
        )
    if statuses.get("offline_target_cache") == "fail":
        actions.append(
            DoctorAction(
                "OFFLINE_TARGET_CACHE_UNVERIFIED",
                "rerun doctor with --offline --connect to verify target cache inputs",
            )
        )
    config = _codex_config(requirements) if agent == "codex" else None
    return OperationResult.success(
        "doctor",
        DoctorValue(
            source,
            target,
            mode,
            tuple(checks),
            tuple(requirements),
            tuple(actions),
            connected,
            scheduler_probe,
            agent,
            config,
        ),
    )


def _usage(message: str) -> OperationResult[DoctorValue]:
    return OperationResult.failure("doctor", OperationError("CLI_USAGE_ERROR", message))


def _package_check() -> DoctorCheck:
    try:
        installed = version("rundra")
    except PackageNotFoundError:
        return DoctorCheck("package", "fail", "Rundra metadata is unavailable")
    return DoctorCheck("package", "pass", f"rundra {installed} is installed")


def _file_check(path: Path, name: str) -> DoctorCheck:
    try:
        with path.open("rb") as stream:
            stream.read(1)
    except FileNotFoundError:
        return DoctorCheck(name, "fail", f"{path} does not exist")
    except OSError:
        return DoctorCheck(name, "fail", f"{path} is not readable")
    return DoctorCheck(name, "pass", f"{path} is readable")


def _read_path_check(path: Path, name: str) -> DoctorCheck:
    candidate = path.expanduser().resolve()
    if candidate.is_file():
        return _file_check(candidate, name)
    try:
        with os.scandir(candidate) as entries:
            next(entries, None)
    except OSError:
        return DoctorCheck(name, "fail", f"{candidate} is not readable")
    return DoctorCheck(name, "pass", f"{candidate} is readable")


def _directory_check(path: Path, name: str, write_probe: bool) -> DoctorCheck:
    directory = path.expanduser().resolve()
    if not write_probe:
        candidate = directory
        while not candidate.exists() and candidate != candidate.parent:
            candidate = candidate.parent
        status = (
            "warning"
            if candidate.is_dir() and os.access(candidate, os.W_OK)
            else "fail"
        )
        return DoctorCheck(name, status, f"{directory} write access was not exercised")
    created: list[Path] = []
    probe: Path | None = None
    try:
        missing: list[Path] = []
        candidate = directory
        while not candidate.exists() and candidate != candidate.parent:
            missing.append(candidate)
            candidate = candidate.parent
        for item in reversed(missing):
            item.mkdir(mode=0o700)
            created.append(item)
        descriptor, raw = tempfile.mkstemp(prefix=".rundra-doctor-", dir=directory)
        probe = Path(raw)
        token = uuid.uuid4().hex.encode("ascii")
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(token)
            stream.flush()
            os.fsync(stream.fileno())
        if probe.read_bytes() != token:
            raise OSError("readback mismatch")
        return DoctorCheck(name, "pass", f"{directory} passed a reversible write probe")
    except OSError:
        return DoctorCheck(name, "fail", f"{directory} failed a reversible write probe")
    finally:
        if probe is not None:
            probe.unlink(missing_ok=True)
        for item in reversed(created):
            try:
                item.rmdir()
            except OSError:
                break


def _guide_check(path: Path) -> DoctorCheck:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError:
        return DoctorCheck(
            "agent_guide",
            "warning",
            "Rundra guidance is absent; run rundr agent-guide --write AGENTS.md",
        )
    installed = (
        content.count("<!-- rundra-agent:start -->") == 1
        and content.count("<!-- rundra-agent:end -->") == 1
    )
    return DoctorCheck(
        "agent_guide",
        "pass" if installed else "warning",
        "Rundra agent guidance is installed"
        if installed
        else "Rundra agent guidance is missing or malformed",
    )


def _target_requirements(target: Target, connected: bool) -> list[DoctorRequirement]:
    if target.transport.kind == "local":
        return []
    host = str(target.transport.options["host"])
    requirements = [
        DoctorRequirement(
            "network",
            host,
            "connect",
            "reach the execution target",
            "local",
            "satisfied" if connected else "untested",
        )
    ]
    config = target.transport.options.get("config_file")
    if isinstance(config, str):
        requirements.append(
            DoctorRequirement(
                "filesystem", config, "read", "configure OpenSSH", "local", "satisfied"
            )
        )
    socket = os.environ.get("SSH_AUTH_SOCK")
    if socket:
        requirements.append(
            DoctorRequirement(
                "unix_socket",
                socket,
                "connect",
                "use SSH-agent authentication",
                "local",
                "satisfied" if connected else "untested",
            )
        )
    requirements.append(
        DoctorRequirement(
            "filesystem",
            str(target.workspace),
            "write",
            "stage remote Runs",
            "target",
            "untested",
        )
    )
    return requirements


def _local_target_access_checks(
    target: Target,
    storage: PreparationStorageConfig,
    write_probe: bool,
) -> tuple[list[DoctorCheck], list[DoctorRequirement]]:
    workspace = Path(target.workspace)
    cache = Path(
        storage.cache_root
        if storage.cache_root is not None
        else PurePath(target.workspace) / "cache"
    )
    checks: list[DoctorCheck] = []
    requirements: list[DoctorRequirement] = []
    for name, path, purpose in (
        (
            "local_target_workspace",
            workspace,
            "access the target workspace from this client",
        ),
        (
            "local_target_preparation_cache",
            cache,
            "reuse target preparation artifacts from this client",
        ),
    ):
        check = _directory_check(path, name, write_probe)
        checks.append(check)
        requirements.append(_path_requirement(path, "write", purpose, check))
    for index, raw_path in enumerate(storage.image_search_paths):
        path = Path(raw_path)
        check = _read_path_check(path, f"local_target_image_search_path_{index}")
        checks.append(check)
        requirements.append(
            _path_requirement(
                path,
                "read",
                "reuse target-visible images from this client",
                check,
            )
        )
    return checks, requirements


def _transport(target: Target) -> SSHTransport:
    config = target.transport.options.get("config_file")
    return SSHTransport(
        str(target.transport.options["host"]),
        executable=str(target.transport.options.get("executable", "ssh")),
        config_file=None if config is None else PurePath(str(config)),
    )


def _staging_roundtrip(target: Target) -> DoctorCheck:
    if target.staging.kind == "shared":
        return _shared_staging_roundtrip(target)
    transport = _transport(target)
    token = uuid.uuid4().hex
    root = PurePath(target.workspace) / f".rundra-doctor-{token}"
    remote_file = root / "token"
    if transport.run(Command(("mkdir", "-m", "700", "--", str(root)))).exit_code != 0:
        return DoctorCheck(
            "staging_roundtrip", "fail", "remote workspace is not writable"
        )
    try:
        with tempfile.TemporaryDirectory(prefix="rundra-doctor-") as temporary:
            local = Path(temporary) / "token"
            retrieved = Path(temporary) / "retrieved"
            local.write_text(token, encoding="ascii")
            ssh = [str(target.transport.options.get("executable", "ssh"))]
            config = target.transport.options.get("config_file")
            if config is not None:
                ssh.extend(("-F", str(config)))
            common = ("rsync", "-a", "--protect-args", "-e", shlex.join(ssh), "--")
            host = str(target.transport.options["host"])
            upload = subprocess.run(
                (*common, str(local), f"{host}:{remote_file}"),
                check=False,
                capture_output=True,
                timeout=30,
                shell=False,
            )
            download = subprocess.run(
                (*common, f"{host}:{remote_file}", str(retrieved)),
                check=False,
                capture_output=True,
                timeout=30,
                shell=False,
            )
            if (
                upload.returncode != 0
                or download.returncode != 0
                or retrieved.read_text(encoding="ascii") != token
            ):
                raise OSError("round trip failed")
    except (OSError, subprocess.TimeoutExpired):
        return DoctorCheck("staging_roundtrip", "fail", "staging round trip failed")
    finally:
        script = 'rm -f -- "$1"; rmdir -- "$2"'
        transport.run(
            Command(("sh", "-c", script, "rundr-doctor", str(remote_file), str(root)))
        )
    return DoctorCheck("staging_roundtrip", "pass", "staging round trip succeeded")


def _shared_staging_roundtrip(target: Target) -> DoctorCheck:
    transport = _transport(target)
    token = uuid.uuid4().hex
    root = Path(target.workspace) / f".rundra-doctor-{token}"
    local_file = root / "token"
    try:
        root.mkdir(mode=0o700)
        local_file.write_text(token, encoding="ascii")
        result = transport.run(Command(("cat", "--", str(local_file))))
        if result.exit_code != 0 or result.stdout != token:
            raise OSError("shared readback failed")
    except OSError:
        return DoctorCheck(
            "staging_roundtrip",
            "fail",
            "shared staging is not visible from both client and target",
        )
    finally:
        local_file.unlink(missing_ok=True)
        try:
            root.rmdir()
        except OSError:
            pass
    return DoctorCheck(
        "staging_roundtrip",
        "pass",
        "shared client-to-target staging round trip succeeded",
    )


def _remote_workspace_requirement(
    target: Target, check: DoctorCheck
) -> DoctorRequirement:
    return DoctorRequirement(
        "filesystem",
        str(target.workspace),
        "write",
        "stage remote Runs",
        "target",
        "satisfied" if check.status == "pass" else "unsatisfied",
    )


def _scheduler_probe(target: Target, timeout: int) -> DoctorCheck:
    remote = target.transport.kind != "local"
    transport: LocalTransport | SSHTransport = (
        _transport(target) if remote else LocalTransport()
    )
    token = uuid.uuid4().hex
    root = (
        PurePath(target.workspace) / f".rundra-doctor-scheduler-{token}"
        if remote
        else PurePath(tempfile.mkdtemp(prefix="rundra-doctor-scheduler-"))
    )
    marker = root / "marker"
    hostname = root / "hostname"
    scheduler: Scheduler | None = None
    reference: SchedulerReference | None = None
    try:
        if (
            remote
            and transport.run(
                Command(("mkdir", "-m", "700", "--", str(root)))
            ).exit_code
        ):
            raise RuntimeError("workspace unavailable")
        scheduler = scheduler_for_target(target, transport, log_directory=root)
        script = 'printf "%s" "$1" > "$2"; hostname > "$3"'
        command_arguments = (token, str(marker), str(hostname))
        if target.execution_storage is not None:
            script = """\
set -eu
token=$1
marker=$2
hostname_path=$3
environment_name=$4
eval "scratch_base=\${$environment_name-}"
case "$scratch_base" in
  /*) ;;
  *) exit 71 ;;
esac
[ "$scratch_base" != / ]
[ -d "$scratch_base" ]
[ -w "$scratch_base" ]
[ ! -L "$scratch_base" ]
scratch_directory=$scratch_base/rundra-doctor-$token
cleanup() {
  rm -f -- "$scratch_directory/marker"
  rmdir -- "$scratch_directory" 2>/dev/null || true
}
trap cleanup EXIT HUP INT TERM
mkdir -m 700 -- "$scratch_directory"
printf "%s" "$token" > "$scratch_directory/marker"
temporary_marker=$marker.tmp-$token
cp -- "$scratch_directory/marker" "$temporary_marker"
mv -f -- "$temporary_marker" "$marker"
hostname > "$hostname_path"
"""
            command_arguments = (
                token,
                str(marker),
                str(hostname),
                target.execution_storage.cpu_environment,
            )
        unit = SchedulerUnit(
            TaskId("task_000000"),
            Command(
                ("sh", "-c", script, "rundr-doctor", *command_arguments)
            ),
            ResourceRequest(
                cpus_per_task=1,
                memory_bytes=256 * 1024 * 1024,
                walltime=timedelta(seconds=60),
            ),
        )
        reference = scheduler.submit(SchedulerGroup((unit,))).reference
        deadline = time.monotonic() + timeout
        observation = scheduler.query((reference,))[0]
        while observation.state not in _TERMINAL and time.monotonic() < deadline:
            time.sleep(1)
            observation = scheduler.query((reference,))[0]
        if observation.state not in _TERMINAL:
            scheduler.cancel((reference,))
            reference = None
            return DoctorCheck(
                "scheduler_probe",
                "warning",
                "scheduler accepted the probe but it did not finish before timeout",
            )
        reference = None
        result = transport.run(Command(("cat", "--", str(marker))))
        if observation.state != ExecutionState.SUCCEEDED or result.stdout != token:
            raise RuntimeError("marker unavailable")
        message = "scheduler submission and compute-side workspace access succeeded"
        if target.execution_storage is not None:
            message += (
                "; allocation-local scratch write, copy-back, and cleanup succeeded "
                f"via {target.execution_storage.cpu_environment}"
            )
        return DoctorCheck("scheduler_probe", "pass", message)
    except Exception:
        return DoctorCheck(
            "scheduler_probe", "fail", "scheduler submission probe failed"
        )
    finally:
        if reference is not None and scheduler is not None:
            try:
                scheduler.cancel((reference,))
            except Exception:
                pass
        script = 'rm -f -- "$1" "$2"; rmdir -- "$3"'
        transport.run(
            Command(
                (
                    "sh",
                    "-c",
                    script,
                    "rundr-doctor",
                    str(marker),
                    str(hostname),
                    str(root),
                )
            )
        )


def _path_requirement(
    path: Path, access: str, purpose: str, check: DoctorCheck
) -> DoctorRequirement:
    status = (
        "satisfied"
        if check.status == "pass"
        else "unsatisfied"
        if check.status == "fail"
        else "untested"
    )
    return DoctorRequirement(
        "filesystem", str(path.expanduser().resolve()), access, purpose, "local", status
    )


def _actions(
    checks: list[DoctorCheck], connected: bool, scheduler_probed: bool
) -> tuple[DoctorAction, ...]:
    actions = [
        DoctorAction(f"FIX_{check.name.upper()}", check.message)
        for check in checks
        if check.status == "fail"
    ]
    if not connected and any(check.name == "remote_access" for check in checks):
        actions.append(DoctorAction("RUN_CONNECT_PROBE", "rerun with --connect"))
    if connected and not scheduler_probed:
        actions.append(
            DoctorAction("OPTIONAL_SCHEDULER_PROBE", "use --scheduler-probe if wanted")
        )
    if any(
        check.name == "agent_guide" and check.status == "warning" for check in checks
    ):
        actions.append(
            DoctorAction(
                "INSTALL_AGENT_GUIDE",
                "run rundr agent-guide --write AGENTS.md, then rerun doctor",
            )
        )
    return tuple(actions)


def _codex_config(requirements: list[DoctorRequirement]) -> str:
    files: dict[str, str] = {}
    hosts: set[str] = set()
    sockets: set[str] = set()
    for item in requirements:
        if item.location != "local":
            continue
        if item.kind == "filesystem":
            current = files.get(item.value)
            files[item.value] = (
                "write" if item.access == "write" or current == "write" else "read"
            )
        elif item.kind == "network":
            hosts.add(item.value.rsplit("@", 1)[-1].split(":", 1)[0])
        elif item.kind == "unix_socket":
            sockets.add(item.value)
    lines = [
        "[permissions.rundra]",
        'description = "Run and retrieve Rundra experiments"',
        'extends = ":workspace"',
        "",
        "[permissions.rundra.filesystem]",
    ]
    lines.extend(
        f"{json.dumps(path)} = {json.dumps(access)}"
        for path, access in sorted(files.items())
    )
    if hosts or sockets:
        lines.extend(
            ("", "[permissions.rundra.network]", "enabled = true", 'mode = "limited"')
        )
    if hosts:
        lines.extend(
            ("allow_local_binding = true", "", "[permissions.rundra.network.domains]")
        )
        lines.extend(f'{json.dumps(host)} = "allow"' for host in sorted(hosts))
    if sockets:
        lines.extend(("", "[permissions.rundra.network.unix_sockets]"))
        lines.extend(f'{json.dumps(path)} = "allow"' for path in sorted(sockets))
    return "\n".join(lines) + "\n"
