import platform

import pytest

import getml


@pytest.mark.skipif(
    platform.system() != "Linux",
    reason=(
        "Testing 'set_project_implicit_launch' is only supported on Linux "
        "(as launching getML from python is only supported with native binaries."
    ),
)
@pytest.mark.skipif(
    getml.engine.is_monitor_alive(),
    reason=(
        "getML is already running. "
        "To test 'set_project_implicit_launch', please save your current session "
        "and stop the running getML instance."
    ),
)
def test_set_project_implicit_launch(request):
    assert not getml.communication.is_monitor_alive()
    getml.set_project(request.node.name)
    assert getml.communication.is_monitor_alive()
    getml.engine.shutdown()
