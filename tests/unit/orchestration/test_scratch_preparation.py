from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

from rundra.domain.models import Command
from rundra.domain.storage import SlurmScratchPolicy
from rundra.orchestration import preparation
from rundra.ports import StagedWorkspace


def test_scratch_preparation_does_not_overwrite_sealed_scheduler_metadata(
    tmp_path: Path,
) -> None:
    root = tmp_path / "run"
    directories = {
        name: root / name
        for name in ("source", "input", "runtime", "output", "logs", "metadata")
    }
    for directory in directories.values():
        directory.mkdir(parents=True)
    scheduler_script = directories["metadata"] / "slurm-array-tasks.sh"
    scheduler_script.write_text("sealed\n", encoding="utf-8")
    scheduler_script.chmod(stat.S_IRUSR)
    workspace = StagedWorkspace(
        root=root,
        source=directories["source"],
        inputs=directories["input"],
        config=directories["input"] / "config.yaml",
        runtime=directories["runtime"],
        outputs=directories["output"],
        logs=directories["logs"],
        metadata=directories["metadata"],
    )
    command = preparation._scratch_preparation_command(
        Command(
            (
                "/bin/sh",
                "-c",
                "printf 'reuse_image_cache\\n' > "
                '"$rundra_run_root/metadata/preparation-actions.tsv"',
            )
        ),
        workspace,
        SlurmScratchPolicy(),
    )
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    environment = {
        **os.environ,
        "SLURM_JOB_ID": "123",
        "SLURM_TMPDIR": str(scratch),
    }

    completed = subprocess.run(command.argv, check=False, env=environment)

    assert completed.returncode == 0
    assert scheduler_script.read_text(encoding="utf-8") == "sealed\n"
    assert stat.S_IMODE(scheduler_script.stat().st_mode) == stat.S_IRUSR
    preparation_result = directories["metadata"] / "preparation-actions.tsv"
    assert preparation_result.read_text(encoding="utf-8") == "reuse_image_cache\n"
    assert stat.S_IMODE(preparation_result.stat().st_mode) == stat.S_IRUSR
