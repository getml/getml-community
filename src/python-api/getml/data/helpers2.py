# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Helper functions that depend on the DataFrame class."""

import warnings
from inspect import cleandoc
from typing import Any, Dict, List, Tuple, Union

from getml.constants import ANNOTATIONS_DOCS_URL, MULTIPLE_JOIN_KEYS_BEGIN, NO_JOIN_KEY
from getml.data.columns import (
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    _parse,
)
from getml.data.data_frame import DataFrame
from getml.data.data_model import DataModel
from getml.data.diagram import _split_multiple_join_keys
from getml.data.helpers import _make_id, list_data_frames
from getml.data.placeholder import Placeholder
from getml.data.roles import target
from getml.data.view import View
from getml.log import logger

# --------------------------------------------------------------------


def _decode_data_model(cmd: Dict[str, Any]):
    def decode(elem):
        return (
            [_decode_placeholder(e) for e in elem]
            if isinstance(elem, list)
            else _decode_placeholder(elem)
        )

    population = _decode_placeholder(cmd)
    peripheral = (
        {k: decode(v) for (k, v) in cmd["peripherals_"].items()}
        if "peripherals_" in cmd
        else {}
    )
    data_model = DataModel(population)
    data_model.peripheral = peripheral
    return data_model


# --------------------------------------------------------------------


def _decode_multiple_join_keys(jk_used, other_jk_used):
    jks1 = _split_multiple_join_keys(jk_used)
    jks2 = _split_multiple_join_keys(other_jk_used)

    if len(jks1) != len(jks2):
        raise ValueError(
            "Number of multiple join keys does not match: "
            + str(jks1)
            + " vs. "
            + str(jks2)
            + "."
        )

    return list(zip(jks1, jks2))


# --------------------------------------------------------------------


def _decode_placeholder(dictionary):
    ph_new = Placeholder(dictionary["name_"], dictionary.get("roles_", None))

    def ts_map(ts):
        return None if ts == ("", "") else ts

    def jk_map(jk):
        if MULTIPLE_JOIN_KEYS_BEGIN in jk[0]:
            return _decode_multiple_join_keys(jk[0], jk[1])
        return None if jk == (NO_JOIN_KEY, NO_JOIN_KEY) else jk

    for i, perph in enumerate(dictionary["joined_tables_"]):
        perph_new = _decode_placeholder(perph)

        ph_new.join(
            perph_new,
            on=jk_map(
                (
                    dictionary["join_keys_used_"][i],
                    dictionary["other_join_keys_used_"][i],
                )
            ),
            time_stamps=ts_map(
                (
                    dictionary["time_stamps_used_"][i],
                    dictionary["other_time_stamps_used_"][i],
                )
            ),
            upper_time_stamp=dictionary["upper_time_stamps_used_"][i],
            relationship=dictionary["relationship_"][i],
            memory=dictionary["memory_"][i],
            horizon=dictionary["horizon_"][i],
            lagged_targets=dictionary["allow_lagged_targets_"][i],
        )

    return ph_new


# -----------------------------------------------------------------


def _deep_copy(df_or_view, container_id):
    if isinstance(df_or_view, DataFrame):
        return df_or_view.copy("container-" + container_id + "-" + df_or_view.name)

    if isinstance(df_or_view, View):
        return df_or_view.to_df("container-" + container_id + "-" + _make_id())


# -----------------------------------------------------------------


def _get_last_change(population, peripheral, subsets):
    return max(
        ([population.last_change] if population is not None else [])
        + [v.last_change for (k, v) in peripheral.items()]
        + [v.last_change for (k, v) in subsets.items()]
    )


# -----------------------------------------------------------------


def _make_subsets_from_kwargs(train, validation, test, **kwargs):
    tvt = {
        "train": train,
        "validation": validation,
        "test": test,
    }
    tvt = {k: v for (k, v) in tvt.items() if v is not None}
    return {**tvt, **kwargs}


# -----------------------------------------------------------------


def _make_subsets_from_split(
    population: Union[DataFrame, View], split: Union[StringColumn, StringColumnView]
) -> Dict[str, Tuple[int, Union[DataFrame, View]]]:
    nrows = len(population)
    unique = set(split[:nrows].unique())  # type: ignore

    subsets = {}

    for name in unique:
        len_set = int((split[:nrows] == name).as_num().sum())
        subset_data = population.drop(population.roles.unused)[split == name]
        if not subset_data:
            msg = f"\nSubset '{name}' is empty."
            if not subset_data.columns:
                msg = cleandoc(
                    """
                    {msg} Have you forgotten to set the roles? Refer to
                    {annotations_docs_url} for more information on
                    annotating data for getML.
                    """
                ).format(msg=msg, annotations_docs_url=ANNOTATIONS_DOCS_URL)

            warnings.warn(msg, UserWarning)
        else:
            subsets[name] = (len_set, subset_data)

    if not subsets:
        raise ValueError("All subsets are empty.")

    return subsets


# -----------------------------------------------------------------


def load_data_frame(name: str) -> DataFrame:
    """Retrieves a [`DataFrame`][getml.DataFrame] handler of data in the
    getML Engine.

    A data frame object can be loaded regardless if it is held in
    memory or not. It only has to be present in the current project
    and thus listed in the output of
    [`list_data_frames`][getml.data.list_data_frames].

    Args:
        name:
            Name of the data frame.

    Returns:
            Handle the underlying data frame in the getML Engine.

    ??? example
        ```python
        d, _ = getml.datasets.make_numerical(population_name = 'test')
        d2 = getml.data.load_data_frame('test')
        ```
    """

    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    data_frames_available = list_data_frames()

    if name in data_frames_available["in_memory"]:
        return DataFrame(name).refresh()

    if name in data_frames_available["on_disk"]:
        return DataFrame(name).load()

    raise ValueError(
        "No data frame holding the name '" + name + "' present on the getML Engine."
    )


# --------------------------------------------------------------------


def _load_dependent_data_frames(cmd):
    if "df_name_" in cmd:
        load_data_frame(cmd["df_name_"])
    for _, value in cmd.items():
        if isinstance(value, dict):
            _load_dependent_data_frames(value)


# --------------------------------------------------------------------


def _load_added(cmd):
    if "added_" not in cmd:
        return None
    _load_dependent_data_frames(cmd["added_"]["col_"])
    col = _parse(cmd["added_"]["col_"])
    added = cmd["added_"]
    added["col_"] = col
    return added


# --------------------------------------------------------------------


def _load_subselection(cmd):
    if "subselection_" not in cmd:
        return None
    _load_dependent_data_frames(cmd["subselection_"])
    return _parse(cmd["subselection_"])


# --------------------------------------------------------------------


def _load_view(cmd):
    typ = cmd["type_"]

    if typ == "DataFrame":
        return load_data_frame(cmd["name_"])

    if typ != "View":
        raise ValueError("Unknown type: '" + typ + "'.")

    added = _load_added(cmd)
    subselection = _load_subselection(cmd)
    dropped = cmd["dropped_"]
    name = cmd["name_"]
    base = _load_view(cmd["base_"])

    return View(
        base=base, name=name, subselection=subselection, added=added, dropped=dropped
    )


# --------------------------------------------------------------------


def exists(name: str):
    """
    Returns true if a data frame named 'name' exists.

    Args:
        name:
            Name of the data frame.
    """
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    all_df = list_data_frames()

    return name in (all_df["in_memory"] + all_df["on_disk"])


# --------------------------------------------------------------------


def delete(name: str):
    """
    If a data frame named 'name' exists, it is deleted.

    Args:
        name:
            Name of the data frame.
    """

    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    if exists(name):
        DataFrame(name).delete()


# --------------------------------------------------------------------


def make_target_columns(base: Union[DataFrame, View], colname: str) -> View:
    """
    Returns a view containing binary target columns.

    getML expects binary target columns for classification problems. This
    helper function allows you to split up a column into such binary
    target columns.

    Args:
        base:
            The original view or data frame. `base` will remain unaffected
            by this function, instead you will get a view with the appropriate
            changes.

        colname: The column you would like to split. A column named
            `colname` should appear on `base`.

    Returns:
        A view containing binary target columns.
    """
    if not isinstance(
        base[colname], (FloatColumn, FloatColumnView, StringColumn, StringColumnView)
    ):
        raise TypeError(
            "'"
            + colname
            + "' must be a FloatColumn, a FloatColumnView, "
            + "a StringColumn or a StringColumnView."
        )

    unique_values = base[colname].unique()

    if len(unique_values) > 10:
        logger.warning(
            "You are splitting the column into more than 10 target "
            + "columns. This might take a long time to fit."
        )

    view = base

    for label in unique_values:
        col = (base[colname] == label).as_num()
        name = colname + "=" + label
        view = view.with_column(col=col, name=name, role=target)

    return view.drop(colname)


# --------------------------------------------------------------------


def to_placeholder(
    *args: Union[DataFrame, View, List[Union[DataFrame, View]]],
    **kwargs: Union[DataFrame, View, List[Union[DataFrame, View]]],
) -> List[Placeholder]:
    """
    Factory function for extracting placeholders from a
    [`DataFrame`][getml.DataFrame] or [`View`][getml.data.View].

    Args:
        args:
            The data frames or views you would like to convert to placeholders.

        kwargs:
            The data frames or views you would like to convert to placeholders.

    Returns:
        A list of placeholders.

    ??? example
        Suppose we wanted to create a [`DataModel`][getml.data.DataModel]:



            dm = getml.data.DataModel(
                population_train.to_placeholder("population")
            )

            # Add placeholders for the peripheral tables.
            dm.add(meta.to_placeholder("meta"))
            dm.add(order.to_placeholder("order"))
            dm.add(trans.to_placeholder("trans"))

        But this is a bit repetitive. So instead, we can do
        the following:
        ```python
        dm = getml.data.DataModel(
            population_train.to_placeholder("population")
        )

        # Add placeholders for the peripheral tables.
        dm.add(getml.data.to_placeholder(
            meta=meta, order=order, trans=trans))
        ```
    """

    def to_ph_list(list_or_elem, key=None):
        as_list = list_or_elem if isinstance(list_or_elem, list) else [list_or_elem]
        return [elem.to_placeholder(key) for elem in as_list]

    return [elem for item in args for elem in to_ph_list(item)] + [
        elem for (k, v) in kwargs.items() for elem in to_ph_list(v, k)
    ]


# --------------------------------------------------------------------
