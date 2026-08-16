from __future__ import annotations

import ast
from pathlib import Path


def test_every_production_subprocess_call_explicitly_disables_local_shell() -> None:
    calls: list[tuple[Path, ast.Call]] = []
    for source in Path("src/rundra").rglob("*.py"):
        tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
        calls.extend(
            (source, node)
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "subprocess"
            and node.func.attr == "run"
        )

    assert calls
    for source, call in calls:
        shell = next(
            (keyword.value for keyword in call.keywords if keyword.arg == "shell"),
            None,
        )
        assert isinstance(shell, ast.Constant) and shell.value is False, (
            f"{source}:{call.lineno} must use shell=False"
        )
        assert call.args and not isinstance(call.args[0], ast.Constant), (
            f"{source}:{call.lineno} must pass an argument vector"
        )
