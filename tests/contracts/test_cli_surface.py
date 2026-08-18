from __future__ import annotations

import argparse
import json
from pathlib import Path

from rundra.cli.main import build_parser

_ROOT = Path(__file__).parents[2]


def _surface(parser: argparse.ArgumentParser) -> dict[str, list[str]]:
    return {
        "positionals": [
            action.dest
            for action in parser._actions
            if not action.option_strings and action.dest not in {"command", "help"}
        ],
        "options": sorted(
            option
            for action in parser._actions
            if action.dest != "help"
            for option in action.option_strings
        ),
    }


def test_cli_surface_matches_version_nine_contract() -> None:
    parser = build_parser()
    subparsers = next(
        action
        for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    )
    actual: dict[str, object] = {
        "format_version": 9,
        "program": parser.prog,
        "global_options": _surface(parser)["options"],
        "commands": {
            name: _surface(command_parser)
            for name, command_parser in subparsers.choices.items()
        },
    }
    expected = json.loads(
        (_ROOT / "docs/schemas/cli-surface-v9.json").read_text(encoding="utf-8")
    )

    assert actual == expected
