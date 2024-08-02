# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


import getml
import pytest


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_loans(getml_project):
    dfs = getml.datasets.load_loans()

    assert [df.name for df in dfs] == [  # type: ignore
        "population_train",
        "population_test",
        "order",
        "trans",
        "meta",
    ]

    assert isinstance(dfs[0], getml.DataFrame)  # type: ignore

    assert getml_project.data_frames.in_memory == [
        "meta",
        "order",
        "population_test",
        "population_train",
        "trans",
    ]

    assert dfs[0]["default"].role == getml.data.roles.target  # type: ignore


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_occupancy(getml_project):
    dfs = getml.datasets.load_occupancy()

    assert getml_project.data_frames.in_memory == [
        "population_test",
        "population_train",
        "population_validation",
    ]

    assert dfs[0]["Occupancy"].role == getml.data.roles.target  # type: ignore


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_interstate94(getml_project):
    traffic = getml.datasets.load_interstate94()

    assert getml_project.data_frames.in_memory == [
        "traffic",
    ]


# --------------------------------------------------------------------


@pytest.mark.slow
def test_load_air_pollution(getml_project):
    population = getml.datasets.load_air_pollution()

    assert getml_project.data_frames.in_memory == ["population"]

    assert population["pm2.5"].role == getml.data.roles.target


# --------------------------------------------------------------------
