# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


import platform
from tempfile import TemporaryDirectory

import pytest

import getml


@pytest.mark.skipif(
    getml.engine.is_monitor_alive(),
    reason=(
        "getML is already running. "
        "To test 'shutdown', please save your current session "
        "and stop the running getML instance."
    ),
)
@pytest.mark.skipif(
    platform.system() != "Linux",
    reason=(
        "Testing 'shutdown' is only sypported on Linux "
        "(as launching getML from python is only supported with native binaries."
    ),
)
@pytest.mark.parametrize("loop", range(5))
def test_shutdown(loop):
    with TemporaryDirectory() as tmpdir:
        getml.engine.launch(project_directory=tmpdir)
        project_name = f"test_shutdown_{loop}"
        getml.engine.set_project(project_name)
        getml.engine.delete_project(project_name)
        getml.engine.shutdown()
