from collections.abc import Sequence

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-shoal-system-tests",
        action="store_true",
        default=False,
        help="enable explicitly marked tests that may access the Shoal cluster",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: Sequence[pytest.Item],
) -> None:
    if config.getoption("--run-shoal-system-tests"):
        return
    skip = pytest.mark.skip(
        reason="requires the explicit --run-shoal-system-tests opt-in"
    )
    for item in items:
        if "shoal_system" in item.keywords:
            item.add_marker(skip)
