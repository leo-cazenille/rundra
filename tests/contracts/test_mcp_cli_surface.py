from __future__ import annotations

import json
from pathlib import Path

from rundra.mcp_server import build_argument_parser

_ROOT = Path(__file__).parents[2]


def test_mcp_cli_surface_matches_version_one_contract() -> None:
    parser = build_argument_parser()
    actual = {
        "format_version": 1,
        "program": parser.prog,
        "positionals": [
            action.dest
            for action in parser._actions
            if not action.option_strings and action.dest != "help"
        ],
        "options": sorted(
            option
            for action in parser._actions
            if action.dest != "help"
            for option in action.option_strings
        ),
    }
    expected = json.loads(
        (_ROOT / "docs/schemas/rundr-mcp-surface-v1.json").read_text(encoding="utf-8")
    )

    assert actual == expected
