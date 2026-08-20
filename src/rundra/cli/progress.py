from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from time import monotonic
from typing import IO, Any, Protocol, cast

from rundra.orchestration.progress import (
    ProgressEvent,
    ProgressObserver,
    ProgressPhase,
)


class ProgressUnavailableError(RuntimeError):
    """The explicitly requested progress renderer is unavailable."""


class _ProgressBar(Protocol):
    n: float
    total: float | None

    def update(self, n: float = 1) -> object: ...

    def set_description_str(
        self, desc: str | None = None, refresh: bool = True
    ) -> None: ...

    def set_postfix_str(self, s: str = "", refresh: bool = True) -> None: ...

    def write(
        self,
        s: str,
        file: IO[str] | None = None,
        end: str = "\n",
        nolock: bool = False,
    ) -> None: ...

    def close(self) -> None: ...


_TqdmFactory = Callable[..., _ProgressBar]


class CLIProgressReporter:
    """Render execution feedback on stderr without changing final stdout."""

    def __init__(
        self,
        *,
        verbose: bool,
        progress: bool,
        stream: IO[str],
        tqdm_factory: _TqdmFactory | None = None,
        announce_run: bool = False,
        progress_interval: float = 10.0,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if type(verbose) is not bool or type(progress) is not bool:
            raise TypeError("CLI progress flags must be booleans")
        if type(progress_interval) not in (int, float) or progress_interval <= 0:
            raise ValueError("CLI progress interval must be positive")
        self._verbose = verbose
        self._stream = stream
        self._bar: _ProgressBar | None = None
        self._factory = tqdm_factory
        self._progress = progress
        self._closed = False
        self._announce_run = announce_run
        self._announced_run = False
        self._progress_interval = float(progress_interval)
        self._clock = clock
        self._last_observed: tuple[object, ...] | None = None
        self._last_verbose: tuple[object, ...] | None = None
        self._last_rendered_phase: ProgressPhase | None = None
        self._last_rendered_at: float | None = None
        self._pending_event: ProgressEvent | None = None

    def __call__(self, event: ProgressEvent) -> None:
        if type(event) is not ProgressEvent:
            raise TypeError("CLIProgressReporter requires a ProgressEvent")
        if self._announce_run and not self._announced_run and event.run_id is not None:
            print(f"Run registered: {event.run_id}", file=self._stream, flush=True)
            self._announced_run = True
        signature = _event_signature(event)
        bar = self._ensure_bar(event) if self._progress else None
        line = f"[rundr] {event.phase.value}: {event.message}"
        if self._verbose and signature != self._last_verbose:
            self._last_verbose = signature
            if bar is None:
                print(line, file=self._stream, flush=True)
            else:
                bar.write(line, file=self._stream)
        if bar is not None:
            if signature != self._last_observed:
                self._last_observed = signature
                self._pending_event = event
            pending = self._pending_event
            if pending is not None and self._should_render(pending):
                self._render(pending)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._pending_event is not None:
            self._render(self._pending_event)
        if self._bar is not None:
            self._bar.close()

    def _should_render(self, event: ProgressEvent) -> bool:
        now = self._clock()
        return (
            self._last_rendered_at is None
            or event.phase is not self._last_rendered_phase
            or event.phase is ProgressPhase.COMPLETE
            or event.completed == event.total
            or now - self._last_rendered_at >= self._progress_interval
        )

    def _render(self, event: ProgressEvent) -> None:
        bar = self._bar
        if bar is None:
            return
        bar.total = (
            float(event.total)
            if event.phase is ProgressPhase.COMPLETE
            else max(bar.total or 0.0, float(event.total))
        )
        bar.set_description_str(f"rundr {event.phase.value}", refresh=False)
        bar.update(max(0.0, float(event.completed) - bar.n))
        bar.set_postfix_str(event.message, refresh=True)
        self._last_rendered_phase = event.phase
        self._last_rendered_at = self._clock()
        self._pending_event = None

    def _ensure_bar(self, event: ProgressEvent) -> _ProgressBar:
        if self._bar is None:
            factory = self._factory or _load_tqdm()
            self._bar = factory(
                total=event.total,
                desc="rundr",
                unit="phase",
                dynamic_ncols=True,
                file=self._stream,
                mininterval=self._progress_interval,
            )
        return self._bar


def create_progress_reporter(
    *,
    verbose: bool,
    progress: bool,
    stream: IO[str],
    announce_run: bool = False,
    progress_interval: float = 10.0,
) -> ProgressObserver | None:
    """Create feedback only when explicitly requested by the caller."""
    if not verbose and not progress and not announce_run:
        return None
    return CLIProgressReporter(
        verbose=verbose,
        progress=progress,
        stream=stream,
        announce_run=announce_run,
        progress_interval=progress_interval,
    )


def close_progress_reporter(observer: ProgressObserver | None) -> None:
    if isinstance(observer, CLIProgressReporter):
        observer.close()


def _load_tqdm() -> _TqdmFactory:
    try:
        module = import_module("tqdm")
    except ModuleNotFoundError as error:
        raise ProgressUnavailableError(
            "--progress requires the TQDM dependency; run 'uv sync'"
        ) from error
    factory: Any = module.tqdm
    return cast(_TqdmFactory, factory)


def _event_signature(event: ProgressEvent) -> tuple[object, ...]:
    return (
        event.phase,
        event.completed,
        event.total,
        event.message,
        event.run_id,
    )
