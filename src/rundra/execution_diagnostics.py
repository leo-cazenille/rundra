from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import PurePath

from rundra.domain.models import Command
from rundra.ports import Transport

DIAGNOSTICS_DIRECTORY = "execution-diagnostics"

_DIAGNOSTIC_REASONS = {
    "SCRATCH_UNSET": "The scheduler did not set the configured scratch variable.",
    "SCRATCH_NOT_ABSOLUTE": "The scheduler scratch path is not absolute.",
    "SCRATCH_IS_ROOT": "The scheduler scratch path is the filesystem root.",
    "SCRATCH_SYMLINK": "The scheduler scratch path is a symbolic link.",
    "SCRATCH_NOT_DIRECTORY": "The scheduler scratch path is missing or is not a directory.",
    "SCRATCH_NOT_WRITABLE": "The scheduler scratch directory is not writable.",
    "SCRATCH_SETUP_FAILED": "The worker could not stage its inputs in allocation scratch.",
    "REQUIRED_OUTPUT_MISSING": "The Task did not produce a required regular file.",
}
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,96}\Z")
_READ_LIMIT = 8

# An unavailable diagnostics directory must never mask the original exit code.
WRITE_DIAGNOSTIC = """\
rundra_write_diagnostic() (
  umask 077
  diagnostic_dir=$1
  diagnostic_id=$2
  case "$diagnostic_id" in ''|*[!a-zA-Z0-9_.-]*) exit 0 ;; esac
  mkdir -p -- "$diagnostic_dir" && [ ! -L "$diagnostic_dir" ] || exit 0
  diagnostic_tmp=$(mktemp "$diagnostic_dir/.event.XXXXXX") || exit 0
  diagnostic_node=$(hostname 2>/dev/null) || diagnostic_node=-
  case "$diagnostic_node" in ''|*[!a-zA-Z0-9_.-]*) diagnostic_node=- ;; esac
  printf '1\\t%s\\t%s\\t%s\\t%s\\n' "$diagnostic_id" "$3" "$4" "$diagnostic_node" > "$diagnostic_tmp" &&
    mv -f -- "$diagnostic_tmp" "$diagnostic_dir/$diagnostic_id.tsv" ||
    rm -f -- "$diagnostic_tmp"
) >/dev/null 2>&1
"""

_READ_DIAGNOSTICS = f"""\
root=$1
if [ -L "$root" ]; then exit 1; fi
if [ ! -e "$root" ]; then exit 0; fi
[ -d "$root" ] && [ -r "$root" ] && [ -x "$root" ] || exit 1
count=0
for file in "$root"/worker-*.tsv "$root"/task-*.tsv; do
  [ -f "$file" ] && [ ! -L "$file" ] || continue
  count=$((count + 1))
  [ "$count" -le {_READ_LIMIT} ] || {{ printf '%s\\n' truncated; break; }}
  head -c 384 -- "$file" || exit 1
done
"""

_CHECK_REQUIRED_OUTPUTS = (
    WRITE_DIAGNOSTIC
    + """\
set -eu
output=$1
# The tag keeps the shared metadata path from being remapped to local scratch.
diagnostic_dir=${2#shared:}
task_id=$3
remaining=$4
shift 4
check_file() (
  directory=$1
  relative=$2
  while :; do
    [ -d "$directory" ] && [ ! -L "$directory" ] || exit 1
    case "$relative" in
      */*) directory=$directory/${relative%%/*}; relative=${relative#*/} ;;
      *) [ -f "$directory/$relative" ] && [ ! -L "$directory/$relative" ]; exit $? ;;
    esac
  done
)
run_command() {
  shift "$remaining"
  "$@"
}
run_command "$@" || exit "$?"
while [ "$remaining" -gt 0 ]; do
  if ! check_file "$output" "$1"; then
    rundra_write_diagnostic "$diagnostic_dir" "$task_id" REQUIRED_OUTPUT_MISSING 86 || :
    printf '%s\\n' 'Rundra: a required Task output is missing or is not a regular file' >&2
    exit 86
  fi
  remaining=$((remaining - 1))
  shift
done
"""
)


@dataclass(frozen=True, slots=True)
class ExecutionDiagnostic:
    scope: str
    identifier: str
    code: str
    exit_code: int
    node: str | None

    @property
    def message(self) -> str:
        return _DIAGNOSTIC_REASONS[self.code]


def read_execution_diagnostics(
    transport: Transport, directory: PurePath
) -> tuple[str, tuple[ExecutionDiagnostic, ...], bool]:
    """Read a bounded sample of framework events from the staged workspace."""
    result = transport.run(
        Command(("sh", "-c", _READ_DIAGNOSTICS, "rundra-diagnostics", str(directory)))
    )
    if result.exit_code != 0:
        return "unavailable", (), False
    rows = result.stdout.splitlines()
    events: list[ExecutionDiagnostic] = []
    for row in rows[:_READ_LIMIT]:
        fields = row.split("\t")
        if len(fields) != 5:
            continue
        version, identifier, code, status, node = fields
        if (
            version != "1"
            or not _IDENTIFIER.fullmatch(identifier)
            or not identifier.startswith(("worker-", "task-"))
            or code not in _DIAGNOSTIC_REASONS
            or not re.fullmatch(r"[0-9]{1,3}", status)
            or not 1 <= int(status) <= 255
            or not _IDENTIFIER.fullmatch(node)
        ):
            continue
        scope, key = identifier.split("-", 1)
        events.append(
            ExecutionDiagnostic(
                scope, key, code, int(status), None if node == "-" else node
            )
        )
    return (
        "available" if events else "not_recorded",
        tuple(events),
        len(rows) > _READ_LIMIT,
    )


def required_outputs_command(
    command: Command,
    *,
    output_root: PurePath,
    metadata_root: PurePath,
    task_id: str,
    paths: tuple[str, ...],
    seed: str,
) -> Command:
    if not paths:
        return command
    return Command(
        (
            "sh",
            "-c",
            _CHECK_REQUIRED_OUTPUTS,
            "rundra-required-outputs",
            str(output_root),
            f"shared:{metadata_root / DIAGNOSTICS_DIRECTORY}",
            f"task-{task_id}",
            str(len(paths)),
            *(path.replace("{seed}", seed) for path in paths),
            *command.argv,
        ),
        environment=command.environment,
        working_directory=command.working_directory,
    )
