# /// script
# requires-python = ">=3.12"
# dependencies = ["pyarrow>=18"]
# ///
from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
import hashlib
import tarfile
from collections import defaultdict
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

import pyarrow.feather as feather


@dataclass(frozen=True)
class ShardInput:
    shard: Path
    member: str
    size: int
    sha256: str


type ResultInput = Path | ShardInput


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def mean_ci95(values: list[float]) -> tuple[float, float]:
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean, 0.0
    return mean, 1.96 * statistics.stdev(values) / math.sqrt(len(values))


def linear_slope(xs: list[float], ys: list[float]) -> float:
    x_mean = statistics.fmean(xs)
    y_mean = statistics.fmean(ys)
    denominator = sum((value - x_mean) ** 2 for value in xs)
    return (
        sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys, strict=True))
        / denominator
    )


def coordinate_columns(names: set[str]) -> tuple[str, str]:
    for pair in (("x", "y"), ("pos_x", "pos_y"), ("position_x", "position_y")):
        if set(pair) <= names:
            return pair
    raise RuntimeError(f"No supported position columns: {sorted(names)}")


def run_msd(source: ResultInput) -> tuple[dict[float, float], list[str], int]:
    if isinstance(source, Path):
        table = feather.read_table(source)
        label = str(source)
    else:
        with tarfile.open(source.shard, mode="r:") as archive:
            member = archive.getmember(source.member)
            stream = archive.extractfile(member)
            if stream is None or not member.isfile() or member.size != source.size:
                raise RuntimeError(f"Invalid shard member {source.member}")
            content = stream.read()
        if hashlib.sha256(content).hexdigest() != source.sha256:
            raise RuntimeError(f"Digest mismatch for shard member {source.member}")
        table = feather.read_table(BytesIO(content))
        label = f"{source.shard}:{source.member}"
    names = set(table.column_names)
    x_name, y_name = coordinate_columns(names)
    required = {"time", "robot_id", x_name, y_name}
    if not required <= names:
        raise RuntimeError(f"Missing columns in {label}: {sorted(required - names)}")
    columns = {
        name: table[name].combine_chunks().to_pylist()
        for name in ("time", "robot_id", x_name, y_name)
    }
    initial: dict[int, tuple[float, float]] = {}
    values: dict[float, list[float]] = defaultdict(list)
    for time_value, robot_value, x_value, y_value in zip(
        columns["time"],
        columns["robot_id"],
        columns[x_name],
        columns[y_name],
        strict=True,
    ):
        robot = int(robot_value)
        time = round(float(time_value), 6)
        x = float(x_value)
        y = float(y_value)
        initial.setdefault(robot, (x, y))
        x0, y0 = initial[robot]
        values[time].append((x - x0) ** 2 + (y - y0) ** 2)
    return (
        {time: statistics.fmean(items) for time, items in values.items()},
        table.column_names,
        len(initial),
    )


def nearest(curve: dict[float, float], target: float) -> tuple[float, float]:
    time = min(curve, key=lambda value: abs(value - target))
    return time, curve[time]


def _input_roots(root: Path) -> tuple[Path, Path]:
    reference = root if root.is_file() else root / "rundra-reference.json"
    if reference.is_file():
        document = json.loads(reference.read_text(encoding="utf-8"))
        if document.get("kind") != "rundra-shared-reference":
            raise RuntimeError(f"Unsupported reference manifest: {reference}")
        return Path(document["metadata_root"]), Path(document["output_root"])
    return root / "metadata", root / "output"


def _shard_members(shard_root: Path) -> dict[str, ShardInput]:
    results: dict[str, ShardInput] = {}
    for shard in sorted(shard_root.glob("*.tar")):
        checksum = shard.with_suffix(".tar.sha256")
        fields = checksum.read_text(encoding="ascii").strip().split()
        if len(fields) != 2 or fields[1] != shard.name:
            raise RuntimeError(f"Invalid shard checksum: {checksum}")
        digest = file_sha256(shard)
        if digest != fields[0]:
            raise RuntimeError(f"Shard checksum mismatch: {shard}")
        with tarfile.open(shard, mode="r:") as archive:
            index = archive.extractfile("index.tsv")
            if index is None:
                raise RuntimeError(f"Missing shard index: {shard}")
            lines = index.read().decode("utf-8").splitlines()
        for line in lines[1:]:
            fields = line.split("\t")
            if len(fields) != 4 or fields[0] != "MEMBER":
                continue
            member = fields[1]
            if member.endswith("/data.feather"):
                task_id = member.split("/", 1)[0]
                results[task_id] = ShardInput(
                    shard, member, int(fields[2]), fields[3]
                )
    return results


def discovered_runs(root: Path) -> dict[str, list[tuple[int, ResultInput]]]:
    metadata_root, output_root = _input_roots(root)
    manifest_path = metadata_root / "tasks.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    shard_root = output_root / ".rundra-shards"
    shards = _shard_members(shard_root) if shard_root.is_dir() else {}
    conditions: dict[str, list[tuple[int, ResultInput]]] = defaultdict(list)
    for task in manifest["tasks"]:
        choices = task["parameter_set"]["choices"]
        regime = choices.get("regime")
        if regime not in {"ballistic", "long_tumble"}:
            continue
        output = root / task["output"] / "data.feather"
        if not output.is_file():
            output = output_root / task["task_id"] / "data.feather"
        result: ResultInput = output if output.is_file() else shards[task["task_id"]]
        conditions[regime].append((int(task["seed"]), result))
    return {
        condition: sorted(items, key=lambda item: item[0])
        for condition, items in conditions.items()
    }


def analyze(root: Path, destination: Path) -> dict[str, Any]:
    discovered = discovered_runs(root)
    runs: dict[str, list[dict[float, float]]] = {}
    schemas: dict[str, list[str]] = {}
    robot_counts: dict[str, list[int]] = {}
    for condition in ("ballistic", "long_tumble"):
        items = discovered.get(condition, [])
        if not items:
            raise RuntimeError(f"No runs found for {condition}")
        condition_runs: list[dict[float, float]] = []
        counts: list[int] = []
        for _, path in items:
            curve, schema, count = run_msd(path)
            condition_runs.append(curve)
            schemas.setdefault(condition, schema)
            counts.append(count)
        runs[condition] = condition_runs
        robot_counts[condition] = counts

    common_times = sorted(
        set.intersection(*(set(curve) for values in runs.values() for curve in values))
    )
    if len(runs["ballistic"]) != len(runs["long_tumble"]):
        raise RuntimeError("Conditions must contain the same number of paired seeds")
    ensemble = {
        condition: {
            time: mean_ci95([curve[time] for curve in condition_runs])
            for time in common_times
        }
        for condition, condition_runs in runs.items()
    }
    checkpoints: dict[str, dict[str, dict[str, float]]] = {}
    slopes: dict[str, float] = {}
    for condition, curve in ensemble.items():
        means = {time: value[0] for time, value in curve.items()}
        checkpoints[condition] = {}
        for target in (1.0, 5.0, 10.0, 30.0, 60.0, 120.0):
            time, mean = nearest(means, target)
            checkpoints[condition][str(target)] = {
                "time": time,
                "mean_msd": mean,
                "ci95": curve[time][1],
            }
        early = [
            (time, mean)
            for time, (mean, _) in curve.items()
            if 1 <= time <= 10 and mean > 0
        ]
        slopes[condition] = linear_slope(
            [math.log(time) for time, _ in early],
            [math.log(mean) for _, mean in early],
        )

    paired_ratios: dict[str, dict[str, float]] = {}
    for target in (5.0, 10.0, 30.0, 60.0, 120.0):
        ratios = []
        for ballistic, tumble in zip(
            runs["ballistic"], runs["long_tumble"], strict=True
        ):
            _, ballistic_msd = nearest(ballistic, target)
            _, tumble_msd = nearest(tumble, target)
            ratios.append(ballistic_msd / tumble_msd if tumble_msd > 0 else math.inf)
        mean, ci95 = mean_ci95(ratios)
        paired_ratios[str(target)] = {"mean": mean, "ci95": ci95}

    summary: dict[str, Any] = {
        "method": (
            "Per-robot squared displacement, averaged within runs and across "
            f"{len(runs['ballistic'])} paired seeds."
        ),
        "schemas": schemas,
        "runs": {condition: len(values) for condition, values in runs.items()},
        "robots_per_run": robot_counts,
        "common_time_points": len(common_times),
        "checkpoints": checkpoints,
        "early_log_log_slope_1_to_10s": slopes,
        "paired_ballistic_to_tumble_msd_ratio": paired_ratios,
    }
    destination.mkdir(parents=True, exist_ok=True)
    (destination / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    with (destination / "curves.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(("condition", "time", "mean_msd", "ci95"))
        for condition, curve in ensemble.items():
            for time, (mean, ci95) in curve.items():
                writer.writerow((condition, time, mean, ci95))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze the Pogosim MSD sweep")
    parser.add_argument("--input", type=Path, default=Path("retrieved/msd-120s"))
    parser.add_argument("--output", type=Path, default=Path("derived/msd-120s"))
    arguments = parser.parse_args()
    print(
        json.dumps(analyze(arguments.input, arguments.output), indent=2, sort_keys=True)
    )


if __name__ == "__main__":
    main()
