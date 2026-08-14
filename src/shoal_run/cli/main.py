from __future__ import annotations

import argparse
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level command-line parser."""
    return argparse.ArgumentParser(
        prog="shoal-run",
        description="Portable experiment execution for scientific computing.",
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Run the shoal-run command-line interface."""
    parser = build_parser()
    parser.parse_args(argv)
    return 0
