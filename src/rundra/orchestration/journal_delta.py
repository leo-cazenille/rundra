from __future__ import annotations

import base64
import hashlib
import re
from collections.abc import Mapping

from rundra.domain.models import Command
from rundra.ports import Transport

_NAME = re.compile(r"[A-Za-z0-9_-]+\.tsv(?:\.[A-Za-z0-9_-]+)?\Z")
_LANE_NAME = re.compile(
    r"[A-Za-z0-9_-]+\.lane-[A-Za-z0-9_-]+\.tsv(?:\.[A-Za-z0-9_-]+)?\Z"
)
_MAX_CACHE_BYTES = 64 * 1024**2

_READ_DELTAS = """\
set -eu
for utility in stat head sha256sum awk base64 dd; do
  command -v "$utility" >/dev/null || exit 66
done
root=$1
previous=$2
shift 2
emit() (
  path=$1
  name=${path##*/}
  # Keep the observed inode open across atomic file publication.
  exec 3<"$path"
  file=/proc/self/fd/3
  size=$(stat -L -c %s -- "$file")
  old=$(printf '%s\\n' "$previous" | awk -F '\\t' -v name="$name" '$1 == name {print $2 " " $3; exit}')
  offset=0
  if [ -n "$old" ]; then
    old_size=${old%% *}
    old_digest=${old#* }
    if [ "$old_size" -le "$size" ]; then
      observed=$(head -c "$old_size" -- "$file" | sha256sum)
      if [ "${observed%% *}" = "$old_digest" ]; then offset=$old_size; fi
    fi
  fi
  printf '%s\\t%s\\t%s\\t' "$name" "$offset" "$size"
  if [ "$size" -gt "$offset" ]; then
    dd if="$file" iflag=skip_bytes,count_bytes skip="$offset" count="$((size-offset))" status=none |
      base64 -w0
  fi
  printf '\\n'
)
for native do
  aggregate="$root/$native.tsv"
  if [ -f "$aggregate" ]; then emit "$aggregate"; continue; fi
  for path in "$root/$native".lane-*.tsv "$root/$native".lane-*.tsv.*; do
    if [ -f "$path" ]; then emit "$path"; fi
  done
done
"""


def read_journal_delta(
    transport: Transport,
    root: str,
    references: tuple[str, ...],
    previous: Mapping[str, bytes],
) -> dict[str, bytes] | None:
    """Fetch append suffixes and return the current aggregate-or-lane view."""
    if any(type(content) is not bytes for content in previous.values()):
        return None
    known = "\n".join(
        f"{name}\t{len(content)}\t{hashlib.sha256(content).hexdigest()}"
        for name, content in previous.items()
        if _valid_name(name)
    )
    known = known or "-"
    # Keep one argv value below the usual OpenSSH/Linux argument limit.
    if len(known) > 64 * 1024:
        return None
    result = transport.run(
        Command(
            (
                "/bin/sh",
                "-c",
                _READ_DELTAS,
                "rundra-journal-delta",
                root,
                known,
                *references,
            )
        )
    )
    if result.exit_code == 255:
        raise ValueError(
            "Target transport command failed with exit code 255 while reading compact "
            "bundled Task journals; the Run was not cancelled and may still be active"
        )
    if result.exit_code != 0:
        return None
    snapshot: dict[str, bytes] = {}
    total = 0
    try:
        for line in result.stdout.splitlines():
            name, offset_text, size_text, encoded = line.split("\t")
            if name in snapshot or not _valid_name(name):
                return None
            offset, size = int(offset_text), int(size_text)
            total += size
            if not 0 <= offset <= size or total > _MAX_CACHE_BYTES:
                return None
            prefix = previous.get(name, b"") if offset else b""
            suffix = base64.b64decode(encoded, validate=True)
            if len(prefix) != offset or len(suffix) != size - offset:
                return None
            snapshot[name] = prefix + suffix
    except ValueError:
        return None
    return snapshot


def _valid_name(name: str) -> bool:
    return _NAME.fullmatch(name) is not None or _LANE_NAME.fullmatch(name) is not None
