from collections.abc import Sequence

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-shoal-system-tests",
        action="store_true",
        default=False,
        help="enable explicitly marked tests that may access the Shoal cluster",
    )
    parser.addoption(
        "--run-shoal-cpu-test",
        action="store_true",
        default=False,
        help="enable the bounded Shoal CPU submission test in addition to preflight",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: Sequence[pytest.Item],
) -> None:
    run_system = bool(config.getoption("--run-shoal-system-tests"))
    run_cpu = bool(config.getoption("--run-shoal-cpu-test"))
    skip_system = pytest.mark.skip(
        reason="requires the explicit --run-shoal-system-tests opt-in"
    )
    skip_cpu = pytest.mark.skip(
        reason="requires both Shoal system and CPU submission opt-ins"
    )
    for item in items:
        if "shoal_cpu" in item.keywords and not (run_system and run_cpu):
            item.add_marker(skip_cpu)
        elif "shoal_system" in item.keywords and not run_system:
            item.add_marker(skip_system)
