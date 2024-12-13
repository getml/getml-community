import decimal
import os
import platform
from contextlib import contextmanager
from datetime import datetime, timezone
from inspect import cleandoc
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import pyarrow as pa
import pytest

import getml
from getml.data._io.arrow import MAX_IEEE754_COMPATIBLE_INT


def pytest_addoption(parser):
    parser.addoption(
        "--workdir",
        action="store",
        default=None,
        help="Specify a working directory for tests",
    )


def pytest_collection_modifyitems(config, items):
    for item in items:
        try:
            if "getml_engine" in item.fixturenames:
                item.add_marker(pytest.mark.getml_engine)
                if platform.system() != "Linux" and not getml.engine.is_monitor_alive():
                    engine_skip_mark = pytest.mark.skip(
                        reason="Engine is not running. Please start it manually."
                    )
                    item.add_marker(engine_skip_mark)
        except:
            pass


@pytest.fixture(autouse=True)
def change_workdir(request, monkeypatch):
    workdir = request.config.getoption("--workdir")

    if workdir:
        workdir_path = Path(workdir)

        if not workdir_path.is_dir():
            raise ValueError(f"{workdir} does not exist.")

        monkeypatch.chdir(workdir_path)


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
    if platform.system() != "Linux":
        yield getml
        return

    print("Starting Engine")
    with TemporaryDirectory() as tmpdir:
        getml.engine.launch(project_directory=tmpdir, log=True)

        yield getml

        print("Shutting down Engine")
        getml.engine.shutdown()


@pytest.fixture(scope="function")
def getml_project(request, getml_engine):
    project_name = Path(request.node.name).stem
    print(f"Using project: {project_name}")
    getml_engine.set_project(project_name)

    yield getml.project

    print(f"Deleting project: {project_name}")
    getml_engine.engine.delete_project(project_name)


@pytest.fixture
def csv_file_with_changing_type_in_row_2():
    data = cleandoc(
        """
        time_stamp,join_key,targets,column_01
        1970-01-01 00:00:00.202923,0,0,-0.9284
        1970-01-01 00:00:00.134847,1,1.000001,-0.4343
        """
    )

    with NamedTemporaryFile(mode="w", delete=False) as csv_file:
        csv_file.write(data)
        csv_file.flush()
        yield csv_file.name
        os.remove(csv_file.name)


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


@pytest.fixture
def timestamp_with_utc_tz_batch():
    return pa.RecordBatch.from_arrays(
        [
            pa.array(
                [datetime(2021, 1, 1, tzinfo=timezone.utc)],
                type=pa.timestamp("ns", tz="UTC"),
            )
        ],
        ["utc_time"],
    )


@pytest.fixture
def timestamp_with_non_utc_tz_batch():
    return pa.RecordBatch.from_arrays(
        [
            pa.array(
                [datetime(2021, 1, 1).astimezone(timezone.utc)],
                type=pa.timestamp("ns", tz="PST8PDT"),
            )
        ],
        ["non_utc_time"],
    )


@pytest.fixture
def timestamp_without_tz_batch():
    return pa.RecordBatch.from_arrays(
        [pa.array([datetime(2021, 1, 1)], type=pa.timestamp("ns"))],
        ["no_tz_time"],
    )


@pytest.fixture
def int_batch():
    return pa.RecordBatch.from_arrays(
        [
            pa.array([1]),
            pa.array([MAX_IEEE754_COMPATIBLE_INT]),
            pa.array([MAX_IEEE754_COMPATIBLE_INT + 1]),
        ],
        schema=pa.schema(
            [
                pa.field("ints", pa.int64()),
                pa.field("int64_hits_float64_upper_bound", pa.int64()),
                pa.field("int64_exceeds_float64_upper_bound", pa.int64()),
            ],
        ),
    )


@pytest.fixture
def decimal_batch():
    return pa.RecordBatch.from_arrays(
        [pa.array([decimal.Decimal(1.0)])],
        ["decimals"],
    )


@pytest.fixture
def timestamp_batch(
    timestamp_with_utc_tz_batch,
    timestamp_without_tz_batch,
):
    return pa.RecordBatch.from_arrays(
        [
            timestamp_with_utc_tz_batch[0],
            timestamp_without_tz_batch[0],
        ],
        ["utc_time", "no_tz_time"],
    )
