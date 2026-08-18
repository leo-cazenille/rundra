from __future__ import annotations

import re
import sys
import tarfile
import zipfile
from collections.abc import Iterable
from pathlib import Path, PurePosixPath

_FORBIDDEN_TEXT = (
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bshoal\b",
        r"\bfishvision\b",
        r"\bbigfish\b",
        r"/shoalhome(?:/|\b)",
        r"/var/local/codex(?:/|\b)",
        r"leo\.cazenille@gmail\.com",
        r"run_[0-9a-f]{32}",
        r"BEGIN (?:OPENSSH|RSA|EC|DSA|PGP) PRIVATE KEY",
        r"Authorization:\s*Bearer\s+\S+",
    )
)
_FORBIDDEN_PATH_PARTS = frozenset({".agent", ".agents", "docs", "examples", "tests"})
_TEXT_SUFFIXES = frozenset({"", ".md", ".py", ".toml", ".txt"})


class DistributionAuditError(RuntimeError):
    """A built distribution violates the public artifact policy."""


def audit_distributions(paths: Iterable[Path]) -> None:
    selected = tuple(paths)
    if not selected:
        raise DistributionAuditError("No distributions were provided")
    kinds: set[str] = set()
    for path in selected:
        if path.suffix == ".whl":
            kinds.add("wheel")
            _audit_wheel(path)
        elif path.name.endswith(".tar.gz"):
            kinds.add("sdist")
            _audit_sdist(path)
        else:
            raise DistributionAuditError(f"Unsupported distribution: {path.name}")
    if kinds != {"wheel", "sdist"}:
        raise DistributionAuditError("Exactly one wheel and one sdist are required")


def _audit_wheel(path: Path) -> None:
    with zipfile.ZipFile(path) as archive:
        for item in archive.infolist():
            relative = PurePosixPath(item.filename)
            if not (
                relative.parts
                and (
                    relative.parts[0] == "rundra"
                    or relative.parts[0].startswith("rundra-")
                    and relative.parts[0].endswith(".dist-info")
                )
            ):
                raise DistributionAuditError(
                    f"Wheel contains unexpected path: {item.filename}"
                )
            if not item.is_dir():
                _audit_payload(item.filename, archive.read(item))


def _audit_sdist(path: Path) -> None:
    with tarfile.open(path, mode="r:gz") as archive:
        roots: set[str] = set()
        payloads: set[str] = set()
        contents: dict[str, bytes] = {}
        for item in archive.getmembers():
            relative = PurePosixPath(item.name)
            if not relative.parts:
                continue
            roots.add(relative.parts[0])
            payload = PurePosixPath(*relative.parts[1:])
            payloads.add(payload.as_posix())
            if item.isfile() and payload.parts and not _allowed_sdist_path(payload):
                raise DistributionAuditError(
                    f"Sdist contains unexpected path: {item.name}"
                )
            if item.isfile():
                stream = archive.extractfile(item)
                if stream is None:
                    raise DistributionAuditError(f"Cannot read sdist path: {item.name}")
                content = stream.read()
                contents[payload.as_posix()] = content
                _audit_payload(item.name, content)
        if len(roots) != 1:
            raise DistributionAuditError("Sdist must contain one versioned root")
        required = {"LICENSE", "PKG-INFO", "README-PYPI.md", "pyproject.toml"}
        if missing := required - payloads:
            raise DistributionAuditError(
                f"Sdist is missing required paths: {', '.join(sorted(missing))}"
            )
        original = contents.get("pyproject.toml.orig")
        if original is not None and original != contents.get("pyproject.toml"):
            raise DistributionAuditError(
                "Sdist pyproject.toml.orig must exactly match pyproject.toml"
            )


def _allowed_sdist_path(path: PurePosixPath) -> bool:
    return path.as_posix() in {
        "LICENSE",
        "PKG-INFO",
        "README-PYPI.md",
        "pyproject.toml",
        "pyproject.toml.orig",
    } or path.parts[:2] == ("src", "rundra")


def _audit_payload(name: str, payload: bytes) -> None:
    path = PurePosixPath(name)
    if "AGENTS.md" in path.parts or _FORBIDDEN_PATH_PARTS.intersection(path.parts):
        raise DistributionAuditError(f"Private path is publishable: {name}")
    if path.suffix.casefold() not in _TEXT_SUFFIXES or len(payload) > 4 * 1024 * 1024:
        return
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return
    for pattern in _FORBIDDEN_TEXT:
        if pattern.search(text):
            raise DistributionAuditError(
                f"Forbidden publication content in {name}: {pattern.pattern}"
            )


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    try:
        audit_distributions(Path(value) for value in arguments)
    except (
        DistributionAuditError,
        OSError,
        tarfile.TarError,
        zipfile.BadZipFile,
    ) as error:
        print(f"distribution audit failed: {error}", file=sys.stderr)
        return 1
    print("distribution audit passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
