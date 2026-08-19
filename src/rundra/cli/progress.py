from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
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
    ) -> None:
        if type(verbose) is not bool or type(progress) is not bool:
            raise TypeError("CLI progress flags must be booleans")
        self._verbose = verbose
        self._stream = stream
        self._bar: _ProgressBar | None = None
        self._factory = tqdm_factory
        self._progress = progress
        self._closed = False
        self._announce_run = announce_run
        self._announced_run = False

    def __call__(self, event: ProgressEvent) -> None:
        if type(event) is not ProgressEvent:
            raise TypeError("CLIProgressReporter requires a ProgressEvent")
        if self._announce_run and not self._announced_run and event.run_id is not None:
            print(f"Run registered: {event.run_id}", file=self._stream, flush=True)
            self._announced_run = True
        bar = self._ensure_bar(event) if self._progress else None
        line = f"[rundr] {event.phase.value}: {event.message}"
        if self._verbose:
            if bar is None:
                print(line, file=self._stream, flush=True)
            else:
                bar.write(line, file=self._stream)
        if bar is not None:
            bar.total = (
                float(event.total)
                if event.phase is ProgressPhase.COMPLETE
                else max(bar.total or 0.0, float(event.total))
            )
            bar.set_description_str(f"rundr {event.phase.value}", refresh=False)
            bar.update(max(0.0, float(event.completed) - bar.n))
            bar.set_postfix_str(event.message, refresh=True)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._bar is not None:
            self._bar.close()

    def _ensure_bar(self, event: ProgressEvent) -> _ProgressBar:
        if self._bar is None:
            factory = self._factory or _load_tqdm()
            self._bar = factory(
                total=event.total,
                desc="rundr",
                unit="phase",
                dynamic_ncols=True,
                file=self._stream,
            )
        return self._bar


def create_progress_reporter(
    *, verbose: bool, progress: bool, stream: IO[str], announce_run: bool = False
) -> ProgressObserver | None:
    """Create feedback only when explicitly requested by the caller."""
    if not verbose and not progress and not announce_run:
        return None
    return CLIProgressReporter(
        verbose=verbose,
        progress=progress,
        stream=stream,
        announce_run=announce_run,
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
