# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import json
from typing import Any, Dict

import numpy as np
import pyarrow as pa
import pytest

import getml

# --------------------------------------------------------------------


def test_from_pandas(getml_project, pandas_df):
    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df)

    assert getml.data.list_data_frames()["in_memory"] == ["databert"]
    assert df._unused_names == ["col", "join_key", "time_stamp"]


# --------------------------------------------------------------------


def test_from_pandas_with_roles(getml_project, pandas_df):
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


def test_from_pandas_round_trip_roles(getml_project, pandas_df):
    roles = {
        "join_key": ["join_key"],
        "time_stamp": ["time_stamp"],
        "numerical": ["col"],
    }

    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df, roles=roles)
    pandas_df = df.to_pandas()
    df_reload = getml.DataFrame.from_pandas(name="databert_reload", pandas_df=pandas_df)
    assert df_reload.roles == df.roles


# --------------------------------------------------------------------


def test_to_pandas(getml_project, pandas_df):
    df = getml.DataFrame.from_pandas(name="databert", pandas_df=pandas_df)

    pandas_df_reload = df.to_pandas()
    assert all(pandas_df["join_key"] == pandas_df_reload["join_key"])
    assert all(pandas_df["col"].astype(int) == pandas_df_reload["col"].astype(int))
    assert pandas_df.shape == pandas_df_reload.shape


# --------------------------------------------------------------------


@pytest.mark.parametrize("df", ["df1", "df2", "df3"], indirect=True)
def test_to_pandas_metadata(getml_project, df):
    pandas_df = df.to_pandas()
    metadata = pandas_df.attrs["getml"]
    assert metadata["name"] == df.name
    assert metadata["last_change"] == df.last_change
    assert metadata["roles"] == df.roles.to_dict()


# --------------------------------------------------------------------


def test_from_arrow(getml_project, arrow_table):
    df = getml.DataFrame.from_arrow(arrow_table, name="databert")
    assert getml.data.list_data_frames()["in_memory"] == ["databert"]
    assert set(df.roles.unused) == set(arrow_table.column_names)
    assert df.shape == arrow_table.shape


# --------------------------------------------------------------------


def test_from_arrow_with_roles(getml_project, arrow_table):
    roles = {
        "join_key": ["join_key"],
        "time_stamp": ["time_stamp"],
        "numerical": ["col"],
    }

    df = getml.DataFrame.from_arrow(arrow_table, name="databert", roles=roles)

    assert getml.data.list_data_frames()["in_memory"] == ["databert"]
    assert df._time_stamp_names == ["time_stamp"]
    assert df._numerical_names == ["col"]
    assert df.shape == arrow_table.shape


# --------------------------------------------------------------------


def test_from_arrow_round_trip_roles(getml_project, arrow_table):
    roles = {
        "join_key": ["join_key"],
        "time_stamp": ["time_stamp"],
        "numerical": ["col"],
    }

    df = getml.DataFrame.from_arrow(arrow_table, name="databert", roles=roles)
    arrow_table = df.to_arrow()
    df_reload = getml.DataFrame.from_arrow(arrow_table, name="databert_reload")
    assert df_reload.roles == df.roles


# --------------------------------------------------------------------


def test_to_arrow(getml_project, arrow_table):
    df = getml.DataFrame.from_arrow(arrow_table, name="databert")

    arrow_table_reload = df.to_arrow()
    assert arrow_table.shape == arrow_table_reload.shape
    assert set(arrow_table.column_names) == set(arrow_table_reload.column_names)
    assert arrow_table.equals(arrow_table_reload)


# --------------------------------------------------------------------


@pytest.mark.parametrize("df", ["df1", "df2", "df3"], indirect=True)
def test_to_arrow_metadata(getml_project, df):
    arrow_table = df.to_arrow()
    metadata = arrow_table.schema.metadata[b"getml"]
    metadata_unmarshaled = json.loads(metadata)
    assert metadata_unmarshaled["name"] == df.name
    assert metadata_unmarshaled["last_change"] == df.last_change
    assert metadata_unmarshaled["roles"] == df.roles.to_dict()


# --------------------------------------------------------------------


def test_read_pandas(getml_project, pandas_df):
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


@pytest.mark.skip("Not implemented yet")
def test_from_db():
    pass


# --------------------------------------------------------------------


def test_from_json(getml_project, json_payload):
    df = getml.DataFrame.from_json(json_payload, name="jason")
    assert df._unused_float_names == ["numbers"]
    assert df._unused_string_names == ["colors"]


# --------------------------------------------------------------------


def test_from_dict(getml_project):
    data: Dict[str, Any] = dict(
        animals=["dog", "cat", "mouse"], weight=[12.2, 125.2, 12], number=[1, 2, 3]
    )

    df = getml.DataFrame.from_dict(data, name="Zoo")

    assert df._unused_string_names == ["animals"]
    assert df._unused_float_names == ["weight", "number"]


# --------------------------------------------------------------------


def test_from_csv(getml_project, csv_file):
    df = getml.DataFrame.from_csv([csv_file], name="Testiana", sep=",")
    assert set(df.roles.unused) == {"column_01", "join_key", "name", "time_stamp"}
    assert (df["column_01"].to_numpy() == np.array([0, 1, 2, 3])).all()


# --------------------------------------------------------------------


def test_list_data_frames(getml_project, pandas_df):
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


def test_load_data_frames(getml_project, pandas_df):
    d = getml.DataFrame.from_pandas(pandas_df, "schnippi")
    d2 = getml.data.load_data_frame("schnippi")
    assert d == d2


# --------------------------------------------------------------------


def test_set_roles(getml_project):
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


def test_compare(getml_project):
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


def test_print(getml_project):
    databert, _ = getml.datasets.make_numerical()
    assert "join_key" in str(databert)


# --------------------------------------------------------------------


def test_delete(getml_project):
    databert, _ = getml.datasets.make_numerical(random_state=123123)
    assert getml.data.list_data_frames()["in_memory"] == [
        "numerical_peripheral_123123",
        "numerical_population_123123",
    ]
    databert.delete()
    assert getml.data.list_data_frames()["in_memory"] == ["numerical_peripheral_123123"]
