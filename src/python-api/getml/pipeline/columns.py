# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Custom class for handling the columns of a pipeline.
"""

from __future__ import annotations

import json
import numbers
import re
from copy import deepcopy
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd
from numpy.typing import NDArray

import getml.communication as comm
from getml.data import Container, StarSchema, TimeSeries
from getml.data.helpers import _is_typed_list
from getml.data.placeholder import Placeholder
from getml.utilities.formatting import _Formatter

from .column import Column
from .helpers import PERIPHERAL, POPULATION, _drop


class Columns:
    """
    Container which holds a pipeline's columns. These include the columns for
    which importance can be calculated, such as the ones with
    [`roles`][getml.data.roles] as [`categorical`][getml.data.roles.categorical],
    [`numerical`][getml.data.roles.numerical] and [`text`][getml.data.roles.text].
    The rest of the columns with roles [`time_stamp`][getml.data.roles.time_stamp],
    [`join_key`][getml.data.roles.join_key], [`target`][getml.data.roles.target],
    [`unused_float`][getml.data.roles.unused_float] and
    [`unused_string`][getml.data.roles.unused_string] can not have importance of course.

    Columns can be accessed by name, index or with a NumPy array. The container
    supports slicing and is sort- and filterable. Further, the container holds
    global methods to request columns' importances and apply a column selection
    to data frames provided to the pipeline.

    Args:
        pipeline:
            The id of the pipeline.
        targets:
            The names of the targets used for this pipeline.
        peripheral:
            The abstract representation of peripheral tables used for this pipeline.
        data:
            The columns to be stored in the container. If not provided, they are obtained from the Engine.

    Note:
        The container is an iterable. So, in addition to
        [`filter`][getml.pipeline.Columns.filter] you can also use python list
        comprehensions for filtering.

    ??? example
        ```python
        all_my_columns = my_pipeline.columns

        first_column = my_pipeline.columns[0]

        all_but_last_10_columns = my_pipeline.columns[:-10]

        important_columns = [column for column in my_pipeline.columns if
        column.importance > 0.1]

        names, importances = my_pipeline.columns.importances()

        # Drops all categorical and numerical columns that are not # in the
        top 20%. new_container = my_pipeline.columns.select(
            container, share_selected_columns=0.2,
        )
        ```
    """

    # ----------------------------------------------------------------

    def __init__(
        self,
        pipeline: str,
        targets: Sequence[str],
        peripheral: Sequence[Placeholder],
        data: Optional[Sequence[Column]] = None,
    ) -> None:
        if not isinstance(pipeline, str):
            raise ValueError("'pipeline' must be a str.")

        if not _is_typed_list(targets, str):
            raise TypeError("'targets' must be a list of str.")

        self.pipeline = pipeline

        self.targets = targets

        self.peripheral = peripheral

        self.peripheral_names = [p.name for p in self.peripheral]

        if data is not None:
            self.data = data
        else:
            self._load_columns()

    # ----------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.data)

    # ----------------------------------------------------------------

    def __iter__(self) -> Iterator[Column]:
        yield from self.data

    # ----------------------------------------------------------------

    def __getitem__(
        self, key: Union[str, int, slice, Union[NDArray[np.int_], NDArray[np.bool_]]]
    ) -> Union[Column, Columns, List[Column]]:
        if not self.data:
            raise AttributeError("Columns container not fully initialised.")

        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            columns_subset = self.data[key]
            return self._make_columns(columns_subset)
        if isinstance(key, str):
            if key in self.names:
                return [column for column in self.data if column.name == key][0]
            raise AttributeError(f"No Column with name: {key}")
        if isinstance(key, np.ndarray):
            columns_subset = np.array(self.data)[key].tolist()
            return list(columns_subset)
        raise TypeError(
            "Columns can only be indexed by: int, slices, or str,"
            f" not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __repr__(self) -> str:
        return self._format()._render_string()

    # ------------------------------------------------------------

    def _repr_html_(self) -> str:
        return self._format()._render_html()

    # ----------------------------------------------------------------

    def _get_column_importances(
        self, target_num: int, sort: bool
    ) -> Tuple[NDArray[np.str_], NDArray[np.float_]]:
        cmd: Dict[str, Any] = {}

        cmd["type_"] = "Pipeline.column_importances"
        cmd["name_"] = self.pipeline

        cmd["target_num_"] = target_num

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.handle_engine_exception(msg)
            msg = comm.recv_string(sock)

        json_obj = json.loads(msg)

        descriptions = np.asarray(json_obj["column_descriptions_"])
        importances = np.asarray(json_obj["column_importances_"])

        if hasattr(self, "data"):
            indices = np.asarray(
                [
                    column.index
                    for column in self.data
                    if column.target == self.targets[target_num]
                    and column.index < len(importances)
                ]
            )

            descriptions = descriptions[indices]
            importances = importances[indices]

        if not sort:
            return descriptions, importances

        indices = np.argsort(importances)[::-1]

        return (descriptions[indices], importances[indices])

    # ----------------------------------------------------------------

    def _format(self) -> _Formatter:
        rows = [
            [
                column.name,
                column.marker,
                column.table,
                column.importance,
                column.target,
            ]
            for column in self.data
        ]

        headers = [
            [
                "name",
                "marker",
                "table",
                "importance",
                "target",
            ]
        ]

        return _Formatter(headers, rows)

    # ----------------------------------------------------------------

    def _load_columns(self) -> None:
        """
        Loads the actual column data from the Engine.
        """
        columns = []

        for target_num, target in enumerate(self.targets):
            descriptions, importances = self._get_column_importances(
                target_num=target_num, sort=False
            )

            columns.extend(
                [
                    Column(
                        index=index,
                        name=description.get("name_"),
                        marker=description.get("marker_"),
                        table=description.get("table_"),
                        importance=importances[index],
                        target=target,
                    )
                    for index, description in enumerate(descriptions)
                ]
            )

        self.data = columns

    # ----------------------------------------------------------------

    def _make_columns(self, data: Sequence[Column]) -> Columns:
        """
        A factory to construct a [`Columns`][getml.pipeline.Columns] container from a list
        of [`Columns`][getml.pipeline.Columns]s.
        """
        return Columns(self.pipeline, self.targets, self.peripheral, data)

    # ----------------------------------------------------------------

    def _pivot(self, field: str) -> Any:
        """
        Pivots the data for a given field. Returns a list of values of the field's type.
        """
        return [getattr(column, field) for column in self.data]

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable[[Column], bool]) -> Columns:
        """
        Filters the columns container.

        Args:
            conditional:
                A callable that evaluates to a boolean for a given item.

        Returns:
            A container of filtered Columns.

        ??? example
            ```python
            important_columns = my_pipeline.columns.filter(lambda column: column.importance > 0.1)
            peripheral_columns = my_pipeline.columns.filter(lambda column: column.marker == "[PERIPHERAL]")
            ```
        """
        columns_filtered = [column for column in self.data if conditional(column)]
        return self._make_columns(columns_filtered)

    # ----------------------------------------------------------------

    def importances(
        self, target_num: int = 0, sort: bool = True
    ) -> Tuple[NDArray[np.str_], NDArray[np.float_]]:
        """
        Returns the data for the column importances.

        Column importances extend the idea of column importances
        to the columns originally inserted into the pipeline.
        Each column is assigned an importance value that measures
        its contribution to the predictive performance. All
        columns importances add up to 1.

        The importances can be calculated for columns with
        [`roles`][getml.data.roles] such as [`categorical`][getml.data.roles.categorical],
        [`numerical`][getml.data.roles.numerical] and [`text`][getml.data.roles.text].
        The rest of the columns with roles [`time_stamp`][getml.data.roles.time_stamp],
        [`join_key`][getml.data.roles.join_key], [`target`][getml.data.roles.target],
        [`unused_float`][getml.data.roles.unused_float] and
        [`unused_string`][getml.data.roles.unused_string] can not have importance of course.

        Args:
            target_num:
                Indicates for which target you want to view the
                importances.
                (Pipelines can have more than one target.)

            sort:
                Whether you want the results to be sorted.

        Returns:
            The first array contains the names of the columns.
            The second array contains their importances. By definition, all importances add up to 1.
        """

        # ------------------------------------------------------------

        descriptions, importances = self._get_column_importances(
            target_num=target_num, sort=sort
        )

        # ------------------------------------------------------------

        names = np.asarray(
            [d["marker_"] + " " + d["table_"] + "." + d["name_"] for d in descriptions]
        )

        # ------------------------------------------------------------

        return names, importances

    # ----------------------------------------------------------------

    @property
    def names(self) -> List[str]:
        """
        Holds the names of a [`Pipeline`][getml.Pipeline]'s columns.

        Returns:
            List containing the names.

        Note:
            The order corresponds to the current sorting of the container.
        """
        return [column.name for column in self.data]

    # ----------------------------------------------------------------

    def select(
        self,
        container: Union[Container, StarSchema, TimeSeries],
        share_selected_columns: float = 0.5,
    ) -> Container:
        """
        Returns a new data container with all insufficiently important columns dropped.

        Args:
            container:
                The container containing the data you want to use.

            share_selected_columns: The share of columns
                to keep. Must be between 0.0 and 1.0.

        Returns:
            A new container with the columns dropped.
        """

        # ------------------------------------------------------------

        if isinstance(container, (StarSchema, TimeSeries)):
            data = self.select(
                container.container, share_selected_columns=share_selected_columns
            )
            new_container = deepcopy(container)
            new_container._container = data
            return new_container

        # ------------------------------------------------------------

        if not isinstance(container, Container):
            raise TypeError(
                "'container' must be a getml.data.Container, "
                + "a getml.data.StarSchema or a getml.data.TimeSeries"
            )

        if not isinstance(share_selected_columns, numbers.Real):
            raise TypeError("'share_selected_columns' must be a real number!")

        if share_selected_columns < 0.0 or share_selected_columns > 1.0:
            raise ValueError("'share_selected_columns' must be between 0 and 1!")

        # ------------------------------------------------------------

        descriptions, _ = self._get_column_importances(target_num=-1, sort=True)

        # ------------------------------------------------------------

        num_keep = int(np.ceil(share_selected_columns * len(descriptions)))

        keep_columns = descriptions[:num_keep]

        # ------------------------------------------------------------

        subsets = {
            k: _drop(v, keep_columns, k, POPULATION)
            for (k, v) in container.subsets.items()
        }

        peripheral = {
            k: _drop(v, keep_columns, k, PERIPHERAL)
            for (k, v) in container.peripheral.items()
        }

        # ------------------------------------------------------------

        new_container = Container(**subsets)
        new_container.add(**peripheral)
        new_container.freeze()

        # ------------------------------------------------------------

        return new_container

    # ----------------------------------------------------------------

    def sort(
        self,
        by: Optional[str] = None,
        key: Optional[Callable[[Column], Any]] = None,
        descending: Optional[bool] = None,
    ) -> Columns:
        """
        Sorts the Columns container. If no arguments are provided the
        container is sorted by target and name.

        Args:
            by:
                The name of field to sort by. Possible fields:
                    - name(s)
                    - table(s)
                    - importances(s)
            key:
                A callable that evaluates to a sort key for a given item.
            descending:
                Whether to sort in descending order.

        Returns:
                A container of sorted columns.

        ??? example
            ```python
            by_importance = my_pipeline.columns.sort(key=lambda column: column.importance)
            ```
        """

        reverse = False if descending is None else descending

        if (by is not None) and (key is not None):
            raise ValueError("Only one of `by` and `key` can be provided.")

        if key is not None:
            columns_sorted = sorted(self.data, key=key, reverse=reverse)
            return self._make_columns(columns_sorted)

        if by is None:
            columns_sorted = sorted(
                self.data, key=lambda column: column.name, reverse=reverse
            )
            columns_sorted.sort(key=lambda column: column.target)
            return self._make_columns(columns_sorted)

        if re.match(pattern="names?$", string=by):
            columns_sorted = sorted(
                self.data, key=lambda column: column.name, reverse=reverse
            )
            return self._make_columns(columns_sorted)

        if re.match(pattern="tables?$", string=by):
            columns_sorted = sorted(
                self.data,
                key=lambda column: column.table,
            )
            return self._make_columns(columns_sorted)

        if re.match(pattern="importances?$", string=by):
            reverse = True if descending is None else descending
            columns_sorted = sorted(
                self.data, key=lambda column: column.importance, reverse=reverse
            )
            return self._make_columns(columns_sorted)

        raise ValueError(f"Cannot sort by: {by}.")

    # ----------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """Returns all information related to the columns in a pandas data frame."""

        names, markers, tables, importances, targets = (
            self._pivot(field)
            for field in ["name", "marker", "table", "importance", "target"]
        )

        data_frame = pd.DataFrame(index=np.arange(len(self.data)))

        data_frame["name"] = names

        data_frame["marker"] = markers

        data_frame["table"] = tables

        data_frame["importance"] = importances

        data_frame["target"] = targets

        return data_frame

    # ----------------------------------------------------------------
