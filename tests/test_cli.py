from __future__ import annotations

import subprocess


def test_console_entry_point_displays_help() -> None:
    result = subprocess.run(
        ["shoal-run", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout.startswith("usage: shoal-run")
