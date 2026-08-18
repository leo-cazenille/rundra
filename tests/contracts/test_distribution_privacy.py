from __future__ import annotations

import subprocess
from pathlib import Path

from tools.audit_distribution import audit_distributions

_ROOT = Path(__file__).parents[2]


def test_built_distributions_obey_public_privacy_contract(tmp_path: Path) -> None:
    completed = subprocess.run(
        ("uv", "build", "--out-dir", str(tmp_path)),
        cwd=_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    audit_distributions((*tmp_path.glob("*.whl"), *tmp_path.glob("*.tar.gz")))
