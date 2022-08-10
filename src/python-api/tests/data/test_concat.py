# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

import getml

# --------------------------------------------------------------------


def make_df1():
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


def make_df2():
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


def test_concat():
    project_name = "test_dataframe"
    getml.engine.set_project(project_name)
    df1 = make_df1()
    df2 = make_df2()
    df3 = getml.data.concat("CONCAT1", [df1, df2])
    assert df3.shape[0] == df1.shape[0] + df2.shape[0]
    getml.engine.delete_project(project_name)


# --------------------------------------------------------------------


def test_concat_views():
    project_name = "test_dataframe"
    getml.engine.set_project(project_name)
    df1 = make_df1()
    df2 = make_df2()
    df4 = getml.data.concat("CONCAT2", [df1, df2[:3]])
    assert df4.shape[0] == df1.shape[0] + 3
    getml.engine.delete_project(project_name)


# --------------------------------------------------------------------

if __name__ == "__main__":
    test_concat()
    test_concat_views()

# --------------------------------------------------------------------
