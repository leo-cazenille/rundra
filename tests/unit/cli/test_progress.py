from __future__ import annotations

from io import StringIO
from typing import IO

from rundra.cli.progress import CLIProgressReporter
from rundra.domain.models import RunId
from rundra.orchestration.progress import ProgressEvent, ProgressPhase


class FakeBar:
    def __init__(self, **options: object) -> None:
        self.options = options
        self.n = 0.0
        self.total = float(options["total"])
        self.descriptions: list[str] = []
        self.postfixes: list[str] = []
        self.lines: list[str] = []
        self.closed = False

    def update(self, n: float = 1) -> None:
        self.n += n

    def set_description_str(
        self, desc: str | None = None, refresh: bool = True
    ) -> None:
        self.descriptions.append(desc or "")

    def set_postfix_str(self, s: str = "", refresh: bool = True) -> None:
        self.postfixes.append(s)

    def write(
        self,
        s: str,
        file: IO[str] | None = None,
        end: str = "\n",
        nolock: bool = False,
    ) -> None:
        self.lines.append(s)

    def close(self) -> None:
        self.closed = True


def test_combined_verbose_progress_reports_details_without_overcounting() -> None:
    stream = StringIO()
    bars: list[FakeBar] = []

    def factory(**options: object) -> FakeBar:
        bar = FakeBar(**options)
        bars.append(bar)
        return bar

    reporter = CLIProgressReporter(
        verbose=True,
        progress=True,
        stream=stream,
        tqdm_factory=factory,
    )
    run_id = RunId("run_0123456789abcdef0123456789abcdef")
    reporter(ProgressEvent(ProgressPhase.RESOLVE, 1, 6, "target=shoal"))
    reporter(ProgressEvent(ProgressPhase.WAIT, 104, 1006, "tasks=100/1000", run_id))
    reporter(ProgressEvent(ProgressPhase.PREPARE, 2, 506, "cache probe", run_id))
    reporter(ProgressEvent(ProgressPhase.WAIT, 504, 1006, "tasks=500/1000", run_id))
    reporter(
        ProgressEvent(ProgressPhase.COMPLETE, 1006, 1006, "state=SUCCEEDED", run_id)
    )
    reporter.close()

    assert len(bars) == 1
    assert bars[0].n == 1006
    assert bars[0].total == 1006
    assert bars[0].closed is True
    assert bars[0].options["total"] == 6
    assert bars[0].descriptions[-1] == "rundr complete"
    assert bars[0].postfixes[-1] == "state=SUCCEEDED"
    assert bars[0].lines == [
        "[rundr] resolve: target=shoal",
        "[rundr] wait: tasks=100/1000",
        "[rundr] prepare: cache probe",
        "[rundr] wait: tasks=500/1000",
        "[rundr] complete: state=SUCCEEDED",
    ]


def test_verbose_without_progress_writes_plain_stderr_lines() -> None:
    stream = StringIO()
    reporter = CLIProgressReporter(verbose=True, progress=False, stream=stream)

    reporter(ProgressEvent(ProgressPhase.PREPARE, 2, 6, "image_action=reuse"))
    reporter.close()

    assert stream.getvalue() == "[rundr] prepare: image_action=reuse\n"


def test_complete_event_defines_terminal_extent_for_submit() -> None:
    stream = StringIO()
    bars: list[FakeBar] = []

    def factory(**options: object) -> FakeBar:
        bar = FakeBar(**options)
        bars.append(bar)
        return bar

    reporter = CLIProgressReporter(
        verbose=False,
        progress=True,
        stream=stream,
        tqdm_factory=factory,
    )
    reporter(ProgressEvent(ProgressPhase.STAGE, 3, 14, "tasks=8"))
    reporter(ProgressEvent(ProgressPhase.COMPLETE, 6, 6, "state=SUBMITTED"))
    reporter.close()

    assert bars[0].n == 6
    assert bars[0].total == 6
