# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import pathlib

import numpy as np
import pytest

import getml.data as data
import getml.data.roles as roles
from getml.data.columns.columns import TIME_STAMP_PARSING_LIKELY_FAILED_WARNING_TEMPLATE


@pytest.mark.parametrize("df_or_view", ["df", "view"], indirect=True)
def test_column_eq_missmatch(getml_project, df_or_view):
    with pytest.raises(TypeError):
        df_or_view.column_01 == "string"
        df_or_view.time_stamp == "string"
        df_or_view.join_key == 1  # join_keys are encoded strings
        df_or_view.time_stamp == "2019-01-01"
        df_or_view.time_stamp.as_str() == 1

    df_or_view.column_01 > 2.4
    df_or_view.time_stamp == 123456789
    df_or_view.join_key == "0"
    df_or_view.targets >= 0


def test_column_operators(getml_project):
    json_str = """{
        "names": ["patrick", "alex", "phil", "ulrike"],
        "column_01": [2.4, 3.0, 1.2, 1.4],
        "join_key": ["0", "1", "2", "3"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
    }"""

    my_df = data.DataFrame(
        "MY DF",
        roles={
            "unused_string": ["names", "join_key", "time_stamp"],
            "unused_float": ["column_01"],
        },
    ).read_json(json_str)

    json_str = """{
        "column_01": [3.4, 2.1, 1.6, 0.8],
        "join_key": ["0", "1", "2", "3"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
    }"""

    my_df2 = data.DataFrame(
        "MY DF2",
        roles={
            "unused_string": ["join_key", "time_stamp"],
            "unused_float": ["column_01"],
        },
    ).read_json(json_str)

    np_arr = np.ones(4)

    my_df3 = data.DataFrame("MY DF3")
    my_df3.add(np_arr, "np_arr")
    my_df3.add(np_arr.astype(str), "str_arr")

    my_df4 = data.DataFrame("MY DF4")
    my_df4.add(np_arr.astype(str), "str_arr")
    my_df4.add(np_arr, "np_arr")

    getting_it_back = my_df3["np_arr"].to_numpy()
    getting_it_back = my_df3["str_arr"].to_numpy()

    my_df.set_role("join_key", roles.join_key)

    # ----------------

    col1 = my_df["column_01"]

    # ----------------

    col2 = 2.0 - col1

    my_df.add(col2, "name", roles.numerical)

    # If you do not explicitly set a role,
    # the assigned role will either be
    # roles.unused_float or roles.unused_string.

    col3 = (col1 + 2.0 * col2) / 3.0

    my_df["column_03"] = col3

    my_df5 = data.DataFrame("MY DF5")
    my_df5["column_03"] = col3

    my_df6 = data.DataFrame("MY DF6")
    my_df6["column_06"] = col3.as_str()

    col4 = col1**-col3

    my_df.add(col4, "column_04", roles.numerical)

    col5 = col1**2.0

    my_df.add(col5, "column_05", roles.numerical)

    col6 = 2.0**col1

    my_df.add(col6, "column_06", roles.numerical)

    col7 = (col1 * 100000.0).sqrt()

    my_df.add(
        col7, name="column_07", role=roles.numerical, unit="time stamp, comparison only"
    )

    col8 = col1 % 0.5

    my_df.add(col8, "column_08", roles.numerical)

    # Operations across different data frames are allowed, as
    # long as these data frames have the same number of rows.

    col9 = (my_df["column_01"] * col8 + my_df2["column_01"] * 3.0) / 5.0

    my_df.add(col9, "column_09", roles.numerical)

    col10 = col1.ceil()

    my_df.add(col10, "column_10", roles.time_stamp)

    col11 = col7.year()

    my_df.add(col11, "year", roles.numerical, unit="year, comparison only")

    col12 = col7.month()

    my_df.add(col12, "month", roles.numerical)

    col13 = col7.day()

    my_df.add(col13, "day", roles.numerical)

    col14 = col7.hour()

    my_df.add(col14, "hour", roles.numerical)

    col15 = col7.minute()

    my_df.add(col15, "minute", roles.numerical)

    col16 = col7.second()

    my_df.add(col16, "second", roles.numerical)

    col17 = col7.weekday()

    my_df.add(col17, "weekday", roles.numerical)

    col18 = col7.yearday()

    my_df.add(col18, "yearday", roles.numerical)

    col19 = my_df["names"]

    col20 = col19.substr(4, 3)

    my_df.add(col20, "short_names", roles.categorical)

    # If you do not explicitly set a role,
    # the assigned role will either be
    # roles.unused_float or roles.unused_string.

    col21 = "user-" + col19 + "-" + col20

    my_df["new_names"] = col21

    col22 = col17.as_str()

    my_df.add(col22, "weekday", roles.categorical)

    col23 = my_df["time_stamp"].as_ts(
        time_formats=["%Y-%m-%dT%H:%M:%s%z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
    )

    my_df.add(col23, "ts", roles.time_stamp)

    # If you get things wrong, you will get a warning
    with pytest.warns(UserWarning) as warnings:
        my_df.add(col21, "ts_null", roles.time_stamp)

    assert str(
        warnings[0].message
    ) == TIME_STAMP_PARSING_LIKELY_FAILED_WARNING_TEMPLATE.format(column="names")

    col24 = col19.contains("rick").as_str()

    my_df.add(col24, "contains 'rick'?", roles.categorical)

    col25 = col19.update(col19.contains("rick"), "Patrick U")

    col25 = col25.update(col19.contains("lex"), "Alex U")

    my_df.add(col25, "update names", roles.categorical)

    home_folder = str(pathlib.Path.home()) + "/"

    my_df.to_csv(home_folder + "MYDF.csv")

    # You can write data frames to the data base - but be
    # careful. Your data base might have stricter naming
    # conventions for your columns than the getml.DataFrames.
    #
    # You can remove any column using DataFrame.drop(...) or del

    my_df.drop(["column_04", "column_05"])

    del my_df["update names"]

    my_df.to_db("MYDF")

    my_view = my_df.where((col1 > 1.3) | (col19 == "alex") | ~(col1 <= 1.3))

    my_other_view = my_df.where(data.random(seed=100) > 0.5)  # type: ignore

    deep_copy = my_df.copy("DEEPCOPY")


def test_as_ts(getml_project, df):
    # StringColumnView.as_ts
    df["ts_parsed"] = df.time_stamp.as_str().as_ts()

    df["ts_str"] = df.time_stamp.as_str()
    # StringColumn.as_ts
    df["ts_str_parsed"] = df.ts_str.as_ts()

    df.set_role(["ts_parsed", "ts_str_parsed"], roles.time_stamp)

    assert "ts_parsed" in df.roles.time_stamp
    assert "ts_str_parsed" in df.roles.time_stamp
    assert df.ts_parsed == df.time_stamp
    assert df.ts_str_parsed == df.time_stamp
