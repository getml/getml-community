from tempfile import TemporaryDirectory
from unittest.mock import patch

import pytest

import getml
from getml.engine._launch import (
    COMPOSE_FILE_URL,
    DOCKER_DOCS_URL,
    PLATFORM_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE,
)


@patch("platform.system")
@pytest.mark.parametrize("system", ["Windows", "Darwin"])
def test_launch_non_native(mock_platform_system, system):
    mock_platform_system.return_value = system
    with pytest.raises(OSError) as exc_info:
        with TemporaryDirectory() as tmpdir:
            getml.engine.launch(project_directory=tmpdir, log=True)
    assert str(
        exc_info.value
    ) == PLATFORM_NOT_SUPPORTED_NATIVELY_ERROR_MSG_TEMPLATE.format(
        platform=system,
        docker_docs_url=DOCKER_DOCS_URL,
        compose_file_url=COMPOSE_FILE_URL,
    )
