from __future__ import annotations

import argparse
import json
import math
import multiprocessing
import os
import queue
import socket
from pathlib import Path
from typing import Any


def _affinity() -> tuple[int, ...] | None:
    getter = getattr(os, "sched_getaffinity", None)
    if getter is None:
        return None
    return tuple(sorted(getter(0)))


def _integrate_partition(
    ordinal: int,
    start: int,
    stop: int,
    intervals: int,
    results: Any,
) -> None:
    width = 1.0 / intervals
    subtotal = 0.0
    for index in range(start, stop):
        midpoint = (index + 0.5) * width
        subtotal += 4.0 / (1.0 + midpoint * midpoint)
    results.put(
        {
            "affinity": _affinity(),
            "ordinal": ordinal,
            "partial_sum": subtotal,
            "pid": os.getpid(),
            "start": start,
            "stop": stop,
        }
    )


def _allocated_cpus() -> int:
    limits: list[int] = []
    slurm_cpus = os.environ.get("SLURM_CPUS_PER_TASK")
    if slurm_cpus is not None:
        try:
            limits.append(int(slurm_cpus))
        except ValueError as error:
            raise ValueError("SLURM_CPUS_PER_TASK must be an integer") from error
    affinity = _affinity()
    if affinity is not None:
        limits.append(len(affinity))
    if not limits:
        detected = os.cpu_count()
        if detected is not None:
            limits.append(detected)
    if not limits or min(limits) < 1:
        raise ValueError("could not determine a positive CPU allocation")
    return min(limits)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=int)
    arguments = parser.parse_args()
    config = json.loads(arguments.config.read_text(encoding="utf-8"))
    processes = config["processes"]
    intervals = config["intervals"]
    if type(processes) is not int or processes < 1:
        raise ValueError("config.processes must be a positive integer")
    if type(intervals) is not int or intervals < processes:
        raise ValueError("config.intervals must be an integer at least processes")
    allocated_cpus = _allocated_cpus()
    if processes > allocated_cpus:
        raise ValueError(
            f"requested {processes} processes but only {allocated_cpus} CPUs "
            "are allocated"
        )

    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    children = []
    for ordinal in range(processes):
        start = intervals * ordinal // processes
        stop = intervals * (ordinal + 1) // processes
        child = context.Process(
            target=_integrate_partition,
            args=(ordinal, start, stop, intervals, results),
        )
        child.start()
        children.append(child)

    partitions: list[dict[str, Any]] = []
    try:
        for _ in children:
            partitions.append(results.get(timeout=300))
    except queue.Empty as error:
        raise RuntimeError("timed out waiting for a child process") from error
    finally:
        for child in children:
            child.join(timeout=10)
    failed = [child.pid for child in children if child.exitcode != 0]
    if failed:
        raise RuntimeError(f"child process failure: {failed}")

    partitions.sort(key=lambda item: item["ordinal"])
    estimate = math.fsum(item["partial_sum"] for item in partitions) / intervals
    document = {
        "absolute_error": abs(estimate - math.pi),
        "allocated_cpus": allocated_cpus,
        "host": socket.gethostname(),
        "intervals": intervals,
        "partitions": partitions,
        "pi_estimate": estimate,
        "processes": processes,
        "seed": arguments.seed,
    }
    destination = Path("../output/results/result.json")
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(document, allow_nan=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
