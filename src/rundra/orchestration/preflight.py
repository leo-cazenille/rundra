from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

from rundra.adapters.slurm import SlurmScriptError, render_sbatch_script
from rundra.domain.models import Command, ExperimentSpec, NativeValue, Target, TaskId
from rundra.ports import (
    CapabilityCheck,
    ContainerRuntime,
    SchedulerGroup,
    SchedulerUnit,
    Transport,
)

_REMOTE_BACKENDS = ("ssh", "slurm", "rsync", "apptainer")
_RESOURCE_CHECK_SCRIPT = """\
set -eu
script=$(mktemp "${TMPDIR:-/tmp}/rundra-preflight.XXXXXX")
trap 'rm -f "$script"' EXIT HUP INT TERM
printf '%s' "$1" > "$script"
"$2" --test-only "$script" >/dev/null
"""


class PreflightStatus(StrEnum):
    """Outcome of one non-submitting infrastructure check."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    BLOCKED = "BLOCKED"


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    """One bounded check with an actionable failure and safe scalar details."""

    name: str
    layer: str
    status: PreflightStatus
    message: str
    corrective_action: str | None = None
    details: Mapping[str, NativeValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("name", "layer", "message"):
            value = getattr(self, name)
            if type(value) is not str or not value.strip():
                raise ValueError(f"PreflightCheck {name} must be nonblank")
        if type(self.status) is not PreflightStatus:
            raise TypeError("PreflightCheck status must be a PreflightStatus")
        if self.corrective_action is not None and (
            type(self.corrective_action) is not str
            or not self.corrective_action.strip()
        ):
            raise ValueError("Preflight corrective action must be nonblank or None")
        if not isinstance(self.details, Mapping) or any(
            type(key) is not str or type(value) not in (str, int, bool)
            for key, value in self.details.items()
        ):
            raise TypeError("Preflight details must contain safe scalar values")
        object.__setattr__(self, "details", MappingProxyType(dict(self.details)))


@dataclass(frozen=True, slots=True)
class PreflightReport:
    """Complete non-submitting check set for one experiment and target."""

    target: str
    experiment: str
    checks: tuple[PreflightCheck, ...]

    def __post_init__(self) -> None:
        if type(self.target) is not str or not self.target.strip():
            raise ValueError("PreflightReport target must be nonblank")
        if type(self.experiment) is not str or not self.experiment.strip():
            raise ValueError("PreflightReport experiment must be nonblank")
        checks = tuple(self.checks)
        if not checks or any(type(check) is not PreflightCheck for check in checks):
            raise ValueError("PreflightReport requires PreflightCheck values")
        if len({check.name for check in checks}) != len(checks):
            raise ValueError("PreflightReport check names must be unique")
        object.__setattr__(self, "checks", checks)

    @property
    def ok(self) -> bool:
        return all(check.status is PreflightStatus.PASSED for check in self.checks)


class RemotePreflight:
    """Inspect the SSH/Slurm/rsync/Apptainer path without submitting work."""

    def __init__(
        self,
        target: Target,
        experiment: ExperimentSpec,
        transport: Transport,
        *,
        rsync_check: Callable[[], CapabilityCheck],
        runtime: ContainerRuntime,
        sbatch: str = "sbatch",
        apptainer: str = "apptainer",
    ) -> None:
        if type(target) is not Target:
            raise TypeError("RemotePreflight target must be a Target")
        if type(experiment) is not ExperimentSpec:
            raise TypeError("RemotePreflight experiment must be an ExperimentSpec")
        if not isinstance(transport, Transport):
            raise TypeError("RemotePreflight transport must implement Transport")
        if not callable(rsync_check):
            raise TypeError("RemotePreflight rsync_check must be callable")
        if not isinstance(runtime, ContainerRuntime):
            raise TypeError("RemotePreflight runtime must implement ContainerRuntime")
        for name, value in (("sbatch", sbatch), ("apptainer", apptainer)):
            if type(value) is not str or not value.strip() or "\x00" in value:
                raise ValueError(f"RemotePreflight {name} must be nonblank and safe")
        self._target = target
        self._experiment = experiment
        self._transport = transport
        self._rsync_check = rsync_check
        self._runtime = runtime
        self._sbatch = sbatch
        self._apptainer = apptainer

    def run(self) -> PreflightReport:
        """Run checks that may read remote state but never allocate or submit."""
        target_check = self._target_check()
        checks = [target_check]
        if target_check.status is not PreflightStatus.PASSED:
            checks.extend(
                self._blocked(name, layer, "target_configuration")
                for name, layer in (
                    ("ssh_client", "transport"),
                    ("rsync_client", "staging"),
                    ("ssh_connectivity", "transport"),
                    ("workspace", "staging"),
                    ("slurm_commands", "scheduler"),
                    ("apptainer_runtime", "container"),
                    ("container_image", "container"),
                    ("requested_resources", "scheduler"),
                    ("shared_filesystem", "staging"),
                )
            )
            return PreflightReport(
                self._target.name, self._experiment.name, tuple(checks)
            )
        ssh_client = self._capability_check(
            "ssh_client",
            "transport",
            self._transport.check,
            "OpenSSH client is available",
            "Install OpenSSH or select an available SSH executable.",
        )
        checks.append(ssh_client)
        checks.append(
            self._capability_check(
                "rsync_client",
                "staging",
                self._rsync_check,
                "Local rsync client is available",
                "Install rsync locally and ensure it is on PATH.",
            )
        )
        if ssh_client.status is not PreflightStatus.PASSED:
            checks.extend(self._blocked_remote_checks("ssh_client"))
            return PreflightReport(
                self._target.name, self._experiment.name, tuple(checks)
            )

        connectivity = self._remote_check(
            "ssh_connectivity",
            "transport",
            Command(("true",)),
            "SSH connection succeeded",
            "Verify the SSH host alias, authentication agent, network access, and host key.",
        )
        checks.append(connectivity)
        if connectivity.status is not PreflightStatus.PASSED:
            checks.extend(self._blocked_remote_checks("ssh_connectivity"))
            return PreflightReport(
                self._target.name, self._experiment.name, tuple(checks)
            )

        workspace = self._workspace_check()
        slurm = self._slurm_commands_check()
        runtime = self._capability_check(
            "apptainer_runtime",
            "container",
            self._runtime.check,
            "Remote Apptainer executable is available",
            "Load or install the site-supported Apptainer module on the SSH host.",
        )
        checks.extend((workspace, slurm, runtime))
        checks.append(
            self._image_check()
            if runtime.status is PreflightStatus.PASSED
            else self._blocked("container_image", "container", "apptainer_runtime")
        )
        checks.append(
            self._resources_check()
            if slurm.status is PreflightStatus.PASSED
            else self._blocked("requested_resources", "scheduler", "slurm_commands")
        )
        checks.append(
            self._shared_filesystem_check()
            if workspace.status is PreflightStatus.PASSED
            else self._blocked("shared_filesystem", "staging", "workspace")
        )
        return PreflightReport(self._target.name, self._experiment.name, tuple(checks))

    def _target_check(self) -> PreflightCheck:
        actual = (
            self._target.transport.kind,
            self._target.scheduler.kind,
            self._target.staging.kind,
            self._target.container.kind,
        )
        if actual != _REMOTE_BACKENDS:
            return _failed(
                "target_configuration",
                "target",
                "Target does not select the supported remote reference path",
                "Use SSH, Slurm, rsync, and Apptainer for the Shoal target.",
                actual="/".join(actual),
            )
        return _passed(
            "target_configuration",
            "target",
            "Target selects SSH, Slurm, rsync, and Apptainer",
        )

    def _workspace_check(self) -> PreflightCheck:
        return self._remote_check(
            "workspace",
            "staging",
            Command(
                (
                    "/bin/sh",
                    "-c",
                    '[ -d "$1" ] && [ -w "$1" ] && [ -x "$1" ]',
                    "rundra-workspace-check",
                    str(self._target.workspace),
                )
            ),
            "Remote workspace exists and is writable/searchable",
            "Create the configured workspace with owner-only write access or select a writable path.",
        )

    def _slurm_commands_check(self) -> PreflightCheck:
        return self._remote_check(
            "slurm_commands",
            "scheduler",
            Command(
                (
                    "/bin/sh",
                    "-c",
                    'for name do command -v -- "$name" >/dev/null || exit 1; done',
                    "rundra-slurm-check",
                    "sbatch",
                    "squeue",
                    "sacct",
                    "scancel",
                )
            ),
            "Required Slurm client commands are available",
            "Load the site Slurm environment providing sbatch, squeue, sacct, and scancel.",
        )

    def _image_check(self) -> PreflightCheck:
        container = self._experiment.container
        if container is None:
            return _failed(
                "container_image",
                "container",
                "Experiment does not declare a container image",
                "Declare an accessible Apptainer image in the experiment.",
            )
        if not container.image.is_absolute():
            return _failed(
                "container_image",
                "container",
                "Preflight cannot inspect a source-relative container image before staging",
                "For Shoal validation, use an absolute image path visible on fishvision.",
                image=str(container.image),
            )
        return self._remote_check(
            "container_image",
            "container",
            Command((self._apptainer, "inspect", "--", str(container.image))),
            "Container image is readable and inspectable by Apptainer",
            "Provide a readable valid Apptainer image at the configured absolute path.",
        )

    def _resources_check(self) -> PreflightCheck:
        group = SchedulerGroup(
            (
                SchedulerUnit(
                    TaskId.from_ordinal(0),
                    Command(("true",)),
                    self._experiment.resources,
                ),
            )
        )
        try:
            script = render_sbatch_script(group)
        except SlurmScriptError as error:
            return _failed(
                "requested_resources",
                "scheduler",
                str(error),
                "Correct the portable resources or allowed resources.native.slurm values.",
            )
        return self._remote_check(
            "requested_resources",
            "scheduler",
            Command(
                (
                    "/bin/sh",
                    "-c",
                    _RESOURCE_CHECK_SCRIPT,
                    "rundra-resource-check",
                    script,
                    self._sbatch,
                )
            ),
            "Slurm accepted the requested resources in test-only mode",
            "Correct account, partition, QOS, constraint, or portable resources for this site.",
        )

    def _shared_filesystem_check(self) -> PreflightCheck:
        return self._remote_check(
            "shared_filesystem",
            "staging",
            Command(
                (
                    "/bin/sh",
                    "-c",
                    'case "$(stat -f -c %T -- "$1")" in nfs|nfs4) exit 0;; *) exit 1;; esac',
                    "rundra-filesystem-check",
                    str(self._target.workspace),
                )
            ),
            "Remote workspace is on an NFS filesystem",
            "Use a /shoalhome workspace shared with compute nodes or verify the site mount.",
        )

    def _capability_check(
        self,
        name: str,
        layer: str,
        check: Callable[[], CapabilityCheck],
        success: str,
        action: str,
    ) -> PreflightCheck:
        try:
            capability = check()
        except Exception:
            return _failed(name, layer, f"{name} capability check failed", action)
        details: dict[str, NativeValue] = {"capability": capability.name}
        if capability.version is not None:
            details["version"] = capability.version
        return _passed(name, layer, success, **details)

    def _remote_check(
        self,
        name: str,
        layer: str,
        command: Command,
        success: str,
        action: str,
    ) -> PreflightCheck:
        try:
            result = self._transport.run(command)
        except Exception:
            return _failed(name, layer, f"Could not run the {name} check", action)
        if result.exit_code != 0:
            return _failed(
                name,
                layer,
                f"{name} check exited unsuccessfully",
                action,
                exit_code=result.exit_code,
            )
        return _passed(name, layer, success)

    def _blocked_remote_checks(self, dependency: str) -> tuple[PreflightCheck, ...]:
        return tuple(
            self._blocked(name, layer, dependency)
            for name, layer in (
                ("workspace", "staging"),
                ("slurm_commands", "scheduler"),
                ("apptainer_runtime", "container"),
                ("container_image", "container"),
                ("requested_resources", "scheduler"),
                ("shared_filesystem", "staging"),
            )
        )

    def _blocked(self, name: str, layer: str, dependency: str) -> PreflightCheck:
        return PreflightCheck(
            name,
            layer,
            PreflightStatus.BLOCKED,
            f"Check blocked by failed {dependency}",
            "Resolve the named dependency, then rerun preflight.",
            {"dependency": dependency},
        )


def _passed(
    name: str,
    layer: str,
    message: str,
    **details: NativeValue,
) -> PreflightCheck:
    return PreflightCheck(
        name,
        layer,
        PreflightStatus.PASSED,
        message,
        details=details,
    )


def _failed(
    name: str,
    layer: str,
    message: str,
    action: str,
    **details: NativeValue,
) -> PreflightCheck:
    return PreflightCheck(
        name,
        layer,
        PreflightStatus.FAILED,
        message,
        action,
        details,
    )
