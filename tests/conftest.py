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
    parser.addoption(
        "--run-shoal-gpu-test",
        action="store_true",
        default=False,
        help="enable the bounded Shoal GPU submission test in addition to preflight",
    )
    parser.addoption(
        "--run-shoal-failure-tests",
        action="store_true",
        default=False,
        help="enable bounded Shoal M4.5 failure scenarios in addition to preflight",
    )
    parser.addoption(
        "--run-shoal-array-test",
        action="store_true",
        default=False,
        help="enable the bounded Shoal M5.6 array test in addition to preflight",
    )
    parser.addoption(
        "--run-shoal-lifecycle-test",
        action="store_true",
        default=False,
        help="enable the bounded Shoal M6.6 async lifecycle test",
    )
    parser.addoption(
        "--run-shoal-pogosim-test",
        action="store_true",
        default=False,
        help="enable the bounded three-seed Pogosim test on Shoal",
    )


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: Sequence[pytest.Item],
) -> None:
    run_system = bool(config.getoption("--run-shoal-system-tests"))
    run_cpu = bool(config.getoption("--run-shoal-cpu-test"))
    run_gpu = bool(config.getoption("--run-shoal-gpu-test"))
    run_failures = bool(config.getoption("--run-shoal-failure-tests"))
    run_array = bool(config.getoption("--run-shoal-array-test"))
    run_lifecycle = bool(config.getoption("--run-shoal-lifecycle-test"))
    run_pogosim = bool(config.getoption("--run-shoal-pogosim-test"))
    skip_system = pytest.mark.skip(
        reason="requires the explicit --run-shoal-system-tests opt-in"
    )
    skip_cpu = pytest.mark.skip(
        reason="requires both Shoal system and CPU submission opt-ins"
    )
    skip_gpu = pytest.mark.skip(
        reason="requires both Shoal system and GPU submission opt-ins"
    )
    skip_failures = pytest.mark.skip(
        reason="requires both Shoal system and M4.5 failure-scenario opt-ins"
    )
    skip_array = pytest.mark.skip(
        reason="requires both Shoal system and M5.6 array submission opt-ins"
    )
    skip_lifecycle = pytest.mark.skip(
        reason="requires both Shoal system and M6.6 lifecycle opt-ins"
    )
    skip_pogosim = pytest.mark.skip(
        reason="requires both Shoal system and Pogosim submission opt-ins"
    )
    for item in items:
        if "shoal_pogosim" in item.keywords and not (run_system and run_pogosim):
            item.add_marker(skip_pogosim)
        elif "shoal_lifecycle" in item.keywords and not (run_system and run_lifecycle):
            item.add_marker(skip_lifecycle)
        elif "shoal_array" in item.keywords and not (run_system and run_array):
            item.add_marker(skip_array)
        elif "shoal_failure" in item.keywords and not (run_system and run_failures):
            item.add_marker(skip_failures)
        elif "shoal_gpu" in item.keywords and not (run_system and run_gpu):
            item.add_marker(skip_gpu)
        elif "shoal_cpu" in item.keywords and not (run_system and run_cpu):
            item.add_marker(skip_cpu)
        elif "shoal_system" in item.keywords and not run_system:
            item.add_marker(skip_system)
