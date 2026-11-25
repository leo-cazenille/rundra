from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path, PurePath

from rundra.adapters.local import LocalTransport
from rundra.cli.operations import _record_ssh_transport, _record_workspace
from rundra.domain.models import ArtifactKind, Command, ConfigSnapshot, TaskId
from rundra.domain.parameters import ParameterSet
from rundra.domain.records import RunRecord
from rundra.domain.sweeps import ExpandedConfig

_CONFIG_LIMIT = 16 * 1024**2
_MANIFEST_LIMIT = 64 * 1024**2


class RetainedInputError(ValueError):
    """An exact staged input cannot be recovered from the saved Run."""


class RetainedInputs:
    """Read exact staged inputs using the original Run's access configuration."""

    def __init__(self, record: RunRecord) -> None:
        self.record = record
        self.workspace = _record_workspace(record)
        sources = (
            artifact.path
            for artifact in record.artifacts
            if artifact.kind is ArtifactKind.SOURCE_SNAPSHOT
        )
        self.source = next(sources, None)
        self.local = (
            record.run.target.transport.kind == "local"
            or record.run.target.staging.kind == "shared"
        )
        self.transport = (
            LocalTransport() if self.local else _record_ssh_transport(record)
        )
        self._rows: list[object] | None = None
        self._configs: dict[int, ExpandedConfig] = {}

    def require_source(self) -> PurePath:
        source = self.source
        if source is None or not source.is_absolute() or source.name != "source":
            raise RetainedInputError("No retained sealed source snapshot is recorded")
        expected = self.workspace.source
        if source != expected and (
            not self.local or source.parent.name != str(self.record.run.id)
        ):
            raise RetainedInputError("Source snapshot does not belong to the saved Run")
        result = self.transport.run(
            Command(
                (
                    "sh",
                    "-c",
                    '[ -d "$1" ] && [ -r "$1" ] && [ -x "$1" ] && [ ! -L "$1" ]',
                    "rundra-retained-source",
                    str(source),
                )
            )
        )
        if result.exit_code != 0:
            raise RetainedInputError(
                "The retained sealed source snapshot is unavailable"
            )
        return source

    def compact_config(self, ordinal: int) -> ExpandedConfig:
        if ordinal in self._configs:
            return self._configs[ordinal]
        space = self.record.task_space
        if space is None or not 0 <= ordinal < space.parameter_set_count:
            raise RetainedInputError(
                "Saved compact Task has an invalid parameter ordinal"
            )
        if self._rows is None:
            raw = self._read(self.workspace.metadata / "tasks.json", _MANIFEST_LIMIT)
            try:
                manifest = json.loads(raw)
                rows = manifest["parameter_sets"]
                if (
                    manifest.get("schema_version") != 2
                    or not isinstance(rows, list)
                    or len(rows) != space.parameter_set_count
                ):
                    raise ValueError
            except (KeyError, ValueError, TypeError, AttributeError) as error:
                raise RetainedInputError(
                    "The retained compact Task manifest is invalid or unsupported"
                ) from error
            self._rows = rows
        item = self._rows[ordinal]
        task_id = TaskId.from_ordinal(ordinal * space.seeds.count)
        filename = f"{task_id}.yaml"
        if (
            not isinstance(item, dict)
            or item.get("ordinal") != ordinal
            or item.get("config") != f"input/{filename}"
        ):
            raise RetainedInputError(
                "Compact parameter rows do not match saved TaskSpace"
            )
        source = self.workspace.inputs / filename
        raw_config = self._read(source, _CONFIG_LIMIT)
        if hashlib.sha256(raw_config).hexdigest() != item.get("config_sha256"):
            raise RetainedInputError(
                f"Retained configuration for parameter ordinal {ordinal} has changed"
            )
        try:
            data = item["parameter_set"]
            parameter_set = (
                None if data is None else ParameterSet(data["id"], data["choices"])
            )
            result = ExpandedConfig(
                ConfigSnapshot(source, raw_config.decode("utf-8")), parameter_set
            )
        except (ValueError, TypeError, KeyError) as error:
            raise RetainedInputError(
                "Retained compact parameter data is invalid"
            ) from error
        self._configs[ordinal] = result
        return result

    def _read(self, path: PurePath, limit: int) -> bytes:
        if self.local:
            local = Path(path)
            try:
                if local.is_file() and not local.is_symlink():
                    with local.open("rb") as stream:
                        result = stream.read(limit + 1)
                    if len(result) <= limit:
                        return result
            except OSError as error:
                raise RetainedInputError(f"Cannot read retained {path.name}") from error
        else:
            response = self.transport.run(
                Command(
                    (
                        "sh",
                        "-c",
                        'set -eu; [ -f "$1" ] && [ ! -L "$1" ] && [ -r "$1" ]; '
                        'size=$(stat -c %s -- "$1"); [ "$size" -le "$2" ]; '
                        'head -c "$(($2 + 1))" -- "$1" | base64 -w0',
                        "rundra-retained-input",
                        str(path),
                        str(limit),
                    )
                )
            )
            if response.exit_code == 0:
                try:
                    result = base64.b64decode(response.stdout, validate=True)
                    if len(result) <= limit:
                        return result
                except ValueError:
                    pass
        raise RetainedInputError(
            f"Retained {path.name} is unavailable or exceeds its limit"
        )
