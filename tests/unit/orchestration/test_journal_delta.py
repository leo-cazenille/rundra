from __future__ import annotations

from pathlib import Path
from time import perf_counter

from rundra.adapters.local import LocalTransport
from rundra.domain.models import Command, RunId
from rundra.domain.scaling import SeedRange, TaskSpace
from rundra.orchestration.journal_delta import read_journal_delta
from rundra.orchestration.service import _full_compact_journal_text
from rundra.persistence import SqliteTaskStore
from rundra.ports import CommandResult


class MeasuredTransport(LocalTransport):
    def __init__(self) -> None:
        self.received = 0

    def run(self, command: Command) -> CommandResult:
        result = super().run(command)
        self.received = len(result.stdout.encode("utf-8"))
        return result


def test_append_poll_reuses_persisted_journal_and_reconciles_replacement(
    tmp_path: Path,
) -> None:
    transport = MeasuredTransport()
    root = tmp_path / "journals"
    root.mkdir()
    lane = root / "19_0.lane-0.tsv"
    initial = b"RUNDRA_TASK_EVENTS\t2\n" + b"".join(
        f"FINISH\ttask_{index:06d}\t0\t0\t1690000000\tcompute1\n".encode()
        for index in range(1000)
    )
    lane.write_bytes(initial)
    previous = read_journal_delta(transport, str(root), ("19_0",), {})
    assert previous == {lane.name: initial}
    store_root = tmp_path / "database"
    run_id = RunId.new()
    store = SqliteTaskStore(store_root)
    store.create(run_id, TaskSpace(1, SeedRange(0, 1000)))
    store.replace_journal_cache(run_id, previous)
    previous = SqliteTaskStore(store_root).journal_cache(run_id)
    suffix = b"FINISH\ttask_001000\t0\t0\t1690000001\tcompute1\n"
    with lane.open("ab") as stream:
        stream.write(suffix)

    started = perf_counter()
    full = transport.run(Command(("cat", "--", str(lane))))
    full_seconds = perf_counter() - started
    full_bytes = transport.received
    started = perf_counter()
    current = read_journal_delta(transport, str(root), ("19_0",), previous)
    delta_seconds = perf_counter() - started
    delta_bytes = transport.received
    assert current == {lane.name: full.stdout.encode()}
    assert delta_bytes < full_bytes // 10
    print(
        f"journal poll full={full_bytes}B/{full_seconds:.4f}s "
        f"delta={delta_bytes}B/{delta_seconds:.4f}s"
    )
    changed = (initial + suffix).replace(b"compute1", b"compute2", 1)
    lane.write_bytes(changed)
    replaced = read_journal_delta(transport, str(root), ("19_0",), current)
    assert replaced == {lane.name: changed}
    aggregate = root / "19_0.tsv"
    aggregate.write_bytes(suffix)
    assert read_journal_delta(transport, str(root), ("19_0",), replaced) == {
        aggregate.name: suffix,
    }


def test_attempt_aggregate_replaces_only_its_lanes(tmp_path: Path) -> None:
    transport = LocalTransport()
    first_lane = tmp_path / "19_0.lane-0.attempt-0.tsv"
    later_lane = tmp_path / "19_0.lane-0.attempt-1.tsv"
    old = b"START\ttask_000000\t0\t1\tcompute1\n"
    new = b"FINISH\ttask_000000\t1\t0\t2\tcompute2\n"
    first_lane.write_bytes(old)
    later_lane.write_bytes(new)
    previous = read_journal_delta(transport, str(tmp_path), ("19_0",), {})
    assert previous == {first_lane.name: old, later_lane.name: new}
    published = tmp_path / "19_0.attempt-0.tsv"
    published.write_bytes(old)
    # The original lane can still exist once the aggregate is published.
    assert read_journal_delta(transport, str(tmp_path), ("19_0",), previous) == {
        published.name: old,
        later_lane.name: new,
    }
    assert (
        _full_compact_journal_text(transport, str(tmp_path), ("19_0",))
        == (old + new).decode()
    )
