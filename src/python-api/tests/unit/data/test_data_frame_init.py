import pytest

import getml


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
