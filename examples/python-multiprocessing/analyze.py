from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from rundra.artifacts import open_result_shard


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--expected-processes", default=4, type=int)
    parser.add_argument("--tolerance", default=1e-10, type=float)
    arguments = parser.parse_args()
    sources = sorted(arguments.input.rglob("result.json"))
    documents: list[tuple[str, object]] = [
        (str(source), json.loads(source.read_text(encoding="utf-8")))
        for source in sources
    ]
    if not documents:
        for archive in sorted(arguments.input.rglob("*.tar")):
            shard = open_result_shard(archive)
            for member in shard.members():
                if member.name == "result.json":
                    documents.append(
                        (
                            f"{archive}:{member}",
                            json.loads(shard.read_bytes(member).decode("utf-8")),
                        )
                    )
    if not documents:
        raise ValueError("input contains no extracted or archived result.json files")

    hosts: set[str] = set()
    seeds: set[int] = set()
    maximum_error = 0.0
    process_total = 0
    for source, result_value in documents:
        if not isinstance(result_value, dict):
            raise ValueError(f"{source} is not a JSON object")
        result = result_value
        partitions = result["partitions"]
        if result["processes"] != arguments.expected_processes:
            raise ValueError(f"{source} has an unexpected process count")
        if len(partitions) != arguments.expected_processes:
            raise ValueError(f"{source} has incomplete process evidence")
        if len({item["pid"] for item in partitions}) != len(partitions):
            raise ValueError(f"{source} did not use distinct child processes")
        ordered = sorted(partitions, key=lambda item: item["ordinal"])
        if ordered[0]["start"] != 0 or ordered[-1]["stop"] != result["intervals"]:
            raise ValueError(f"{source} has incomplete interval coverage")
        if any(
            left["stop"] != right["start"]
            for left, right in zip(ordered, ordered[1:], strict=False)
        ):
            raise ValueError(f"{source} has overlapping or missing intervals")
        error = abs(result["pi_estimate"] - math.pi)
        if not math.isfinite(error) or error > arguments.tolerance:
            raise ValueError(f"{source} exceeds the numerical tolerance")
        maximum_error = max(maximum_error, error)
        process_total += len(partitions)
        hosts.add(result["host"])
        seeds.add(result["seed"])

    summary = {
        "hosts": sorted(hosts),
        "maximum_absolute_error": maximum_error,
        "processes": process_total,
        "seeds": sorted(seeds),
        "tasks": len(documents),
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(
        json.dumps(summary, allow_nan=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, allow_nan=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
