import os
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import getml


def pytest_collection_modifyitems(items):
    for item in items:
        try:
            fixtures = item.fixturenames
            if "getml_engine" in fixtures:
                item.add_marker(pytest.mark.engine)
            if "getml_project" in fixtures:
                item.add_marker(pytest.mark.project)
        except:
            pass


@contextmanager
def workdir(dir: str):
    cwd = Path.cwd()
    os.chdir(dir)
    try:
        yield
    finally:
        os.chdir(cwd)


@pytest.fixture(scope="module")
def getml_engine():
    print("Starting engine")
    with TemporaryDirectory() as tmpdir:
        getml.engine.launch(project_directory=tmpdir, log=True)

        yield getml.engine

        print("Shutting down engine")
        getml.engine.shutdown()


@pytest.fixture(scope="function")
def getml_project(request, getml_engine):
    project_name = Path(request.node.name).stem
    print(f"Using project: {project_name}")
    getml_engine.set_project(project_name)

    yield getml.project

    print(f"Deleting project: {project_name}")
    getml_engine.delete_project(project_name)


@pytest.fixture
def df_or_view(request, df):
    """
    Example data:

        >>> df

        name  time_stamp  join_key  targets  column_01
        role  time_stamp  join_key  target  numerical
        unit  time stamp
        0  1970-01-01 00:00:00.202923  0  0   -0.9284
        1  1970-01-01 00:00:00.134847  1  0   -0.4343
        2  1970-01-01 00:00:00.592543  2  0   -0.7653
        3  1970-01-01 00:00:00.272449  3  0   0.6704
        4  1970-01-01 00:00:00.918001  4  0   0.3387
        5  1970-01-01 00:00:00.825230  5  0   0.728
        6  1970-01-01 00:00:00.541397  6  0   0.56
        7  1970-01-01 00:00:00.688549  7  0   0.8729
        8  1970-01-01 00:00:00.150777  8  0   0.9636
        9  1970-01-01 00:00:00.101308  9  0   -0.8872

        10 rows x 4 columns
    """

    if getattr(request, "param", None) == "view":
        return df[:]

    if getattr(request, "param", "df") == "df":
        return df

    return df


@pytest.fixture
def df():
    """
    Example data:

        >>> df

        name  time_stamp  join_key  targets  column_01
        role  time_stamp  join_key  target  numerical
        unit  time stamp
        0  1970-01-01 00:00:00.202923  0  0   -0.9284
        1  1970-01-01 00:00:00.134847  1  0   -0.4343
        2  1970-01-01 00:00:00.592543  2  0   -0.7653
        3  1970-01-01 00:00:00.272449  3  0   0.6704
        4  1970-01-01 00:00:00.918001  4  0   0.3387
        5  1970-01-01 00:00:00.825230  5  0   0.728
        6  1970-01-01 00:00:00.541397  6  0   0.56
        7  1970-01-01 00:00:00.688549  7  0   0.8729
        8  1970-01-01 00:00:00.150777  8  0   0.9636
        9  1970-01-01 00:00:00.101308  9  0   -0.8872

        10 rows x 4 columns
    """
    df, _ = getml.datasets.make_numerical(n_rows_population=10, n_rows_peripheral=0)
    yield df


@pytest.fixture
def view(df):
    """
    Example data:

        >>> df

        name  time_stamp  join_key  targets  column_01
        role  time_stamp  join_key  target  numerical
        unit  time stamp
        0  1970-01-01 00:00:00.202923  0  0   -0.9284
        1  1970-01-01 00:00:00.134847  1  0   -0.4343
        2  1970-01-01 00:00:00.592543  2  0   -0.7653
        3  1970-01-01 00:00:00.272449  3  0   0.6704
        4  1970-01-01 00:00:00.918001  4  0   0.3387
        5  1970-01-01 00:00:00.825230  5  0   0.728
        6  1970-01-01 00:00:00.541397  6  0   0.56
        7  1970-01-01 00:00:00.688549  7  0   0.8729
        8  1970-01-01 00:00:00.150777  8  0   0.9636
        9  1970-01-01 00:00:00.101308  9  0   -0.8872

        10 rows x 4 columns
    """
    return df[:]


@pytest.fixture
def tmpdir():
    with TemporaryDirectory() as tmpdir:
        with workdir(tmpdir):
            yield tmpdir
