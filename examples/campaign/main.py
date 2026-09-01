import argparse
import json
import random
from pathlib import Path

import yaml

parser = argparse.ArgumentParser()
parser.add_argument("--config", required=True, type=Path)
parser.add_argument("--seed", required=True, type=int)
parser.add_argument("--output", required=True, type=Path)
args = parser.parse_args()

config = yaml.safe_load(args.config.read_text(encoding="utf-8"))
rng = random.Random(args.seed)
result = {
    "seed": args.seed,
    "samples": [rng.random() for _ in range(config["sample_count"])],
}
args.output.parent.mkdir(parents=True, exist_ok=True)
args.output.write_text(json.dumps(result, sort_keys=True) + "\n", encoding="utf-8")
