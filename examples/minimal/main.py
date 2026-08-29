from __future__ import annotations

import argparse
import json
import random
from pathlib import Path

import yaml


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()
    config = yaml.safe_load(arguments.config.read_text(encoding="utf-8"))
    population_size = config["population"]["size"]
    generator = random.Random(arguments.seed)
    result = {
        "population_size": population_size,
        "samples": [generator.random() for _ in range(3)],
        "seed": arguments.seed,
    }
    destination = arguments.output
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(result, allow_nan=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
