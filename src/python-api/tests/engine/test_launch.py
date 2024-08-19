import platform
from unittest.mock import patch

import pytest

import getml
from getml.engine._launch import (
    COMPOSE_FILE_URL,
    DOCKER_DOCS_URL,
    PLATFORM_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE,
)


@patch("getml.engine._launch._is_monitor_alive")
@patch("platform.system")
@pytest.mark.parametrize("system", ["Windows", "Darwin"])
def test_launch_non_native(mock_platform_system, mock_getml_is_monitor_alive, system):
    mock_platform_system.return_value = system
    mock_getml_is_monitor_alive.return_value = False
    with pytest.raises(OSError) as exc_info:
        getml.engine.launch()
    assert str(
        exc_info.value
    ) == PLATFORM_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE.format(
        platform=system,
        docker_docs_url=DOCKER_DOCS_URL,
        compose_file_url=COMPOSE_FILE_URL,
    )


@pytest.mark.skipif(
    platform.system() != "Linux",
    reason=(
        "Testing 'launch' is only sypported on Linux "
        "(as launching getML from python is only supported with native binaries."
    ),
)
@pytest.mark.skipif(
    getml.engine.is_monitor_alive(),
    reason=(
        "getML is already running. "
        "To test 'launch', please save your current session "
        "and stop the running getML instance."
    ),
)
def test_launch():
    getml.engine.launch()
    assert getml.engine.is_monitor_alive()
    getml.engine.shutdown()
    assert not getml.engine.is_monitor_alive()
