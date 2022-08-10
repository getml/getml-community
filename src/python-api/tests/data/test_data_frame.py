# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

import json
import tempfile

import numpy as np
import pandas as pd  # type: ignore
import pytest

import getml


@pytest.fixture
def engine():
    """Test project for each test

    This fixture is injected into each test resulting in a new project. The
    code after 'yield' is exectued at the end of the test.
    """
    project_name = "test_dataframe"
    getml.engine.set_project(project_name)
    yield None
    getml.engine.delete_project(project_name)


# --------------------------------------------------------------------


@pytest.fixture
def pandas_df():
    random = np.random.RandomState(8290)
    pandas_df = pd.DataFrame()
    pandas_df["col"] = random.rand(100) * 2.0 - 1.0
    pandas_df["join_key"] = random.randint(1, 200, 100)
    pandas_df["time_stamp"] = [pd.Timestamp(ii) for ii in random.rand(100) * 1e15]
    return pandas_df


# --------------------------------------------------------------------


@pytest.fixture
def csv_file():
    data = b"""names,column_01,join_key,time_stamp
        patrick, 0, 22, 2019-01-01
        alex, 1, 23, 2019-01-02
        phil, 2, 24, 2019-01-03
        ulrike, 3, 25, 2019-01-04
     """
    csv_file = tempfile.NamedTemporaryFile(delete=True)
    csv_file.write(data)
    csv_file.seek(0)
    yield csv_file.name
    csv_file.close()


# --------------------------------------------------------------------


@pytest.fixture
def json_str():
    json_str = """{
        "colors": ["blue", "green", "red", "yellow"],
        "numbers": [2.4, 3.0, 1.2, 1.4]
    }"""
    return json_str


# --------------------------------------------------------------------


@pytest.fixture
def df1():
    json_str1 = """{
        "names": ["patrick", "alex", "phil", "ulrike"],
        "column_01": [2.4, 3.0, 1.2, 1.4],
        "join_key": ["0", "1", "2", "3"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
    }"""
    my_df1 = getml.DataFrame(
        "MY DF 1",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_json(json_str1)
    return my_df1


# --------------------------------------------------------------------


@pytest.fixture
def df2():
    json_str2 = """{
        "names": ["patrick", "alex", "phil", "johannes", "ulrike", "adil"],
        "column_01": [2.4, 3.0, 1.2, 1.4, 3.4, 2.2],
        "join_key": ["0", "1", "2", "2", "3", "4"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05", "2019-01-06"]
    }"""

    my_df2 = getml.DataFrame(
        "MY DF 2",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_json(json_str2)

    return my_df2


# --------------------------------------------------------------------


@pytest.fixture
def df3():
    json_str = """{
        "names": ["patrick", "alex", "phil", "ulrike", "patrick", "alex", "phil", "ulrike", "NULL"],
        "column_01": [2.4, 3.0, 1.2, 1.4, 3.4, 2.2, 10.2, 13.5, 11.0],
        "join_key": ["0", "1", "2", "2", "3", "3", "4", "4", "4"]
    }"""

    my_df3 = getml.DataFrame(
        "MY DF3",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
        },
    ).read_json(json_str)
    return my_df3


# --------------------------------------------------------------------


def test_init():
    df = getml.DataFrame(name="initbert")
    assert df.name == "initbert"

    roles = dict(
        dummbert=["dumm", "bert"],  # invalid role
        numerical=[1, 2],  # invalid list
        join_key=["join_key"],
        time_stamp=["date_1", "date_2"],
        categorical=["cat_1", "cat_2", "cat_3"],
    )

    with pytest.raises(ValueError):
        df = getml.DataFrame(name="failbert", roles=roles)
    roles.pop("dummbert")

    with pytest.raises(TypeError):
        df = getml.DataFrame(name="failbert", roles=roles)
    roles["numerical"] = ["num_1", "num_2"]

    df = getml.DataFrame(name="rolebert", roles=roles)
    assert df._categorical_names == ["cat_1", "cat_2", "cat_3"]
    assert df._numerical_names == ["num_1", "num_2"]
    assert df._join_key_names == ["join_key"]
    assert df._time_stamp_names == ["date_1", "date_2"]


# --------------------------------------------------------------------


def test_from_pandas(engine, pandas_df):

    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df)

    assert getml.data.list_data_frames()["in_memory"] == ["databert"]
    assert df._unused_names == ["col", "join_key", "time_stamp"]


# --------------------------------------------------------------------


def test_from_pandas_with_roles(engine, pandas_df):

    roles = {
        "join_key": ["join_key"],
        "time_stamp": ["time_stamp"],
        "numerical": ["col"],
    }

    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df, roles=roles)

    assert getml.data.list_data_frames()["in_memory"] == ["databert"]
    assert df._time_stamp_names == ["time_stamp"]
    assert df._numerical_names == ["col"]


# --------------------------------------------------------------------


def test_to_pandas(engine, pandas_df):

    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df)

    pandas_df_reload = df.to_pandas()
    assert all(pandas_df["join_key"] == pandas_df_reload["join_key"])
    assert all(pandas_df["col"].astype(int) == pandas_df_reload["col"].astype(int))
    assert pandas_df.shape == pandas_df_reload.shape


# --------------------------------------------------------------------


def test_read_pandas(engine, pandas_df):

    df = getml.DataFrame(name="testbert")

    with pytest.raises(Exception):
        df.read_pandas(pandas_df)

    df = getml.DataFrame(name="testbert", roles={"numerical": ["col"]})

    df.read_pandas(pandas_df)

    assert df._numerical_names == ["col"]
    assert len(df["col"].to_numpy()) == 100

    df.read_pandas(pandas_df, append=True)

    assert len(df["col"].to_numpy()) == 200


# --------------------------------------------------------------------


def test_from_db(engine):
    pass


# --------------------------------------------------------------------


def test_from_json(engine, json_str):
    df = getml.DataFrame.from_json(json_str, name="jason")
    assert df._unused_float_names == ["numbers"]
    assert df._unused_string_names == ["colors"]


# --------------------------------------------------------------------


def test_from_dict(engine):

    data = dict(
        animals=["dog", "cat", "mouse"], weight=[12.2, 125.2, 12], number=[1, 2, 3]
    )

    df = getml.DataFrame.from_dict(data, name="Zoo")

    assert df._unused_string_names == ["animals"]
    assert df._unused_float_names == ["weight", "number"]


# --------------------------------------------------------------------


def test_from_csv(engine, csv_file):
    df = getml.DataFrame.from_csv([csv_file], name="Testiana", sep=",")
    assert df.roles.unused == ["column_01", "join_key", "time_stamp", "names"]
    assert (df["column_01"].to_numpy() == [0, 1, 2, 3]).all()


# --------------------------------------------------------------------


def test_list_data_frames(engine, pandas_df):
    d = getml.DataFrame.from_pandas(pandas_df, "schnippi")
    assert (
        json.dumps(getml.data.list_data_frames())
        == '{"in_memory": ["schnippi"], "on_disk": []}'
    )
    d.save()
    assert (
        json.dumps(getml.data.list_data_frames())
        == '{"in_memory": ["schnippi"], "on_disk": ["schnippi"]}'
    )


# --------------------------------------------------------------------


def test_load_data_frames(engine, pandas_df):
    d = getml.DataFrame.from_pandas(pandas_df, "schnippi")
    d2 = getml.data.load_data_frame("schnippi")
    assert d == d2


# --------------------------------------------------------------------


def test_set_roles(engine):
    d_num, _ = getml.datasets.make_numerical(n_rows_population=10, n_rows_peripheral=20)
    d_cat, d_cat_2 = getml.datasets.make_categorical(
        n_rows_population=10, n_rows_peripheral=20
    )

    assert d_num._categorical_names == []
    assert d_num._numerical_names == ["column_01"]
    assert d_cat._categorical_names == ["column_01"]
    assert d_cat._numerical_names == []
    assert d_cat_2._categorical_names == ["column_01"]
    assert d_cat_2._numerical_names == []
    assert d_cat_2._time_stamp_names == ["time_stamp"]

    d_num.set_role("column_01", getml.data.roles.categorical)
    d_cat.set_role("column_01", getml.data.roles.numerical)
    d_cat_2.set_role("column_01", getml.data.roles.time_stamp)

    assert d_num._categorical_names == ["column_01"]
    assert d_num._numerical_names == []
    assert d_cat._categorical_names == []
    assert d_cat._numerical_names == ["column_01"]
    assert d_cat_2._categorical_names == []
    assert d_cat_2._numerical_names == []
    assert d_cat_2._time_stamp_names == ["time_stamp", "column_01"]


# --------------------------------------------------------------------


def test_compare(engine):

    databert_pop_1, databert_peri_1 = getml.datasets.make_numerical(
        random_state=829034, n_rows_population=10, n_rows_peripheral=20
    )
    databert_pop_2, databert_peri_2 = getml.datasets.make_numerical(
        random_state=829034, n_rows_population=10, n_rows_peripheral=20
    )
    databert_pop_3, databert_peri_3 = getml.datasets.make_numerical(
        random_state=829, n_rows_population=10, n_rows_peripheral=20
    )

    assert databert_pop_1 == databert_pop_1
    assert databert_peri_1 == databert_peri_1
    assert databert_pop_1 == databert_pop_2
    assert databert_peri_1 == databert_peri_2
    assert databert_pop_3 != databert_pop_2
    assert databert_peri_3 != databert_peri_2


# --------------------------------------------------------------------


def test_print(engine):
    databert, _ = getml.datasets.make_numerical()
    assert "join_key" in str(databert)


# --------------------------------------------------------------------


def test_delete(engine):
    databert, _ = getml.datasets.make_numerical(random_state=123123)
    assert getml.data.list_data_frames()["in_memory"] == [
        "numerical_peripheral_123123",
        "numerical_population_123123",
    ]
    databert.delete()
    assert getml.data.list_data_frames()["in_memory"] == ["numerical_peripheral_123123"]
