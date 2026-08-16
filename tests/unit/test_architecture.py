from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).parents[2] / "src" / "rundra"
_PORTABLE_FORBIDDEN = (
    "rundra.adapters",
    "rundra.cli",
    "rundra.config",
    "rundra.orchestration",
    "rundra.persistence",
    "rundra.provenance",
)
_ORCHESTRATION_FORBIDDEN = (
    "rundra.adapters",
    "rundra.cli",
    "rundra.config",
)


def _imports(source: Path) -> tuple[str, ...]:
    tree = ast.parse(source.read_text(encoding="utf-8"), filename=str(source))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.append(node.module)
    return tuple(imported)


def _violations(source: Path, forbidden: tuple[str, ...]) -> list[str]:
    return [
        imported
        for imported in _imports(source)
        if any(
            imported == prefix or imported.startswith(f"{prefix}.")
            for prefix in forbidden
        )
    ]


def test_portable_domain_and_ports_do_not_import_outer_layers() -> None:
    sources = [
        *_PACKAGE_ROOT.joinpath("domain").glob("*.py"),
        _PACKAGE_ROOT / "ports.py",
    ]
    failures = {
        str(source.relative_to(_PACKAGE_ROOT)): violations
        for source in sources
        if (violations := _violations(source, _PORTABLE_FORBIDDEN))
    }

    assert failures == {}


def test_orchestration_does_not_import_concrete_adapters_or_interfaces() -> None:
    sources = list(_PACKAGE_ROOT.joinpath("orchestration").glob("*.py"))
    failures = {
        str(source.relative_to(_PACKAGE_ROOT)): violations
        for source in sources
        if (violations := _violations(source, _ORCHESTRATION_FORBIDDEN))
    }

    assert failures == {}
