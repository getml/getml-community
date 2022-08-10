# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 



"""
Tests the data frames container.
"""

import numpy as np

from getml import data, datasets, engine, project


def make_populations(base, percentiles):
    population_tables = []

    targets = base["targets"].to_numpy()

    for p in percentiles:
        targets_ = np.clip(targets, 0, np.percentile(targets, p))
        pop_ = base.copy("pop_0" + str(p))
        pop_["targets"] = targets_
        pop_.set_role("targets", data.roles.target)
        population_tables += [pop_]

    return population_tables


def test_data_frames():
    """
    Tests the data frames cotainer.
    """
    # ----------------

    engine.set_project("examples")

    # ----------------
    # Generate artificial datasets

    pop, _ = datasets.make_numerical()
    pop = pop.copy("pop_100")

    # ----------------

    pops = make_populations(pop, range(55, 95, 5))

    # ----------------

    assert (
        len(project.data_frames) == len(pops) + 3
    ), "Expected length of data frames container to equal the length of all populations + 2"

    project.data_frames.save()

    names_in_memory = sorted(df.name for df in project.data_frames)
    names_on_disk = sorted(project.data_frames.on_disk)

    assert (
        names_on_disk == names_in_memory
    ), "Expected names for data frames in memory and in project folder to be the same"

    dfs = project.data_frames.retrieve()

    assert names_in_memory == sorted(
        list(dfs.keys())
    ), "Expected keys of handler to be the same as the names in memory"

    assert all(
        handle == df.name for handle, df in zip(dfs.keys(), dfs.values())
    ), "Expected handle to correspond to the name on engine"

    project.data_frames.unload()

    assert (
        len(project.data_frames.on_disk) > 1
    ), "Expected data frames in project folder"

    assert len(project.data_frames.in_memory) == 0, "Expected no data frames in memory"

    project.data_frames.load()

    assert len(project.data_frames.in_memory) == len(
        names_in_memory
    ), "Expected all data frames to reappear in memory"

    project.data_frames.delete()

    assert (
        len(project.data_frames.on_disk) == 0
    ), "Expected no data frames in project folder"

    # ----------------

    engine.delete_project("examples")


# ----------------


if __name__ == "__main__":
    test_data_frames()
