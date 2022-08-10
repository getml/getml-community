# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 



import pandas as pd  # type: ignore
import pytest

import getml


@pytest.fixture
def engine():
    """Test project for each test

    This fixture is injected into each test resulting in a new project. The
    code after 'yield' is exectued at the end of the test.
    """
    project_name = "test_base"
    getml.engine.set_project(project_name)
    yield None
    getml.engine.delete_project(project_name)


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_loans(engine):
    dfs = getml.datasets.load_loans()

    assert [df.name for df in dfs] == [  # type: ignore
        "population_train",
        "population_test",
        "order",
        "trans",
        "meta",
    ]

    assert isinstance(dfs[0], getml.DataFrame)

    assert getml.project.data_frames.in_memory == [
        "meta",
        "order",
        "population_test",
        "population_train",
        "trans",
    ]

    assert dfs[0]["default"].role == getml.data.roles.target


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_occupancy(engine):
    dfs = getml.datasets.load_occupancy()

    assert getml.project.data_frames.in_memory == [
        "population_test",
        "population_train",
        "population_validation",
    ]

    assert dfs[0]["Occupancy"].role == getml.data.roles.target


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_interstate94(engine):
    traffic = getml.datasets.load_interstate94()

    assert getml.project.data_frames.in_memory == [
        "traffic",
    ]


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_air_pollution(engine):
    population = getml.datasets.load_air_pollution()

    assert getml.project.data_frames.in_memory == ["population"]

    assert population["pm2.5"].role == getml.data.roles.target


# --------------------------------------------------------------------
