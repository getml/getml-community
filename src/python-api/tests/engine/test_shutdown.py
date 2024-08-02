"""
This makes sure that the
engine shutdown works as intended.
"""

from tempfile import TemporaryDirectory
import getml
import pytest


@pytest.mark.parametrize("loop", range(100))
def test_shutdown(loop):
    """
    This makes sure that the
    engine shutdown works as intended.
    """
    with TemporaryDirectory() as tmpdir:
        getml.engine.launch(in_memory=True, project_directory=tmpdir)
        project_name = f"test_shutdown_{loop}"
        getml.engine.set_project(project_name)
        getml.engine.delete_project(project_name)
        getml.engine.shutdown()
