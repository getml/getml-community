# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Contains custom class for handling the tables of a pipeline.
"""

from __future__ import annotations

import re
from typing import Sequence, Optional, Iterator, Union, Tuple, Callable, Any

import numpy as np
from numpy.typing import NDArray
import pandas as pd  # type: ignore

from getml.utilities.formatting import _Formatter

from .table import Table
from .columns import Columns


class Tables:
    """
    This container holds a pipeline's tables. These tables are build from the
    columns for which importances can be calculated. The motivation behind this
    container is to determine which tables are more important than others.

    Tables can be accessed by name, index or with a NumPy array. The container
    supports slicing and can be sorted and filtered. Further, the container
    holds global methods to request tables' importances.

    Note:
        The container is an iterable. So, in addition to
        :meth:`~getml.pipeline.Tables.filter` you can also use python list
        comprehensions for filtering.

    Example:
        .. code-block:: python

            all_my_tables = my_pipeline.tables

            first_table = my_pipeline.tables[0]

            all_but_last_10_tables = my_pipeline.tables[:-10]

            important_tables = [table for table in my_pipeline.tables if
            table.importance > 0.1]

            names, importances = my_pipeline.tables.importances()
    """

    def __init__(
        self,
        targets: Sequence[str],
        columns: Columns,
        data: Optional[Sequence[Table]] = None,
    ) -> None:
        self._targets = targets
        self._columns = columns

        if data is not None:
            self.data = data

        else:
            self._load_tables()

        if not (targets and columns) and not data:
            raise ValueError(
                "Missing required arguments. Either provide `targets` & "
                "`columns` or else provide `data`."
            )

    # ----------------------------------------------------------------

    def __getitem__(
        self, key: Union[str, int, slice, Union[NDArray[np.int_], NDArray[np.bool_]]]
    ) -> Union[Table, Tables, list[Table]]:
        if not self.data:
            raise AttributeError("Tables container not fully initialized.")

        if isinstance(key, int):
            return self.data[key]

        if isinstance(key, slice):
            tables_subset = self.data[key]
            return self._make_tables(tables_subset)

        if isinstance(key, str):
            if key in self.names:
                return [table for table in self.data if table.name == key][0]
            raise AttributeError(f"No Table with name: {key}")

        if isinstance(key, np.ndarray):
            tables_subset = np.array(self.data)[key].tolist()
            return self._make_tables(tables_subset)

        raise TypeError(
            "Columns can only be indexed by: int, slices, str or np.ndarray,"
            f" not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __iter__(self) -> Iterator[Table]:
        yield from self.data

    # ----------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.data)

    # ----------------------------------------------------------------

    def __repr__(self) -> str:
        return self._format()._render_string()

    # ------------------------------------------------------------

    def _repr_html_(self) -> str:
        return self._format()._render_html()

    # ----------------------------------------------------------------

    def _format(self) -> _Formatter:
        headers = [["name", "importance", "target", "marker"]]

        rows = [
            [
                table.name,
                table.importance,
                table.target,
                table.marker,
            ]
            for table in self.data
        ]

        return _Formatter(headers, rows)

    # ----------------------------------------------------------------

    def _load_tables(self) -> None:
        """
        Gets tables data from columns
        """

        tables = []

        for table_target in self._targets:
            importances: dict[str, float] = {
                column.table: 0.0
                for column in self._columns
                if column.target == table_target
            }

            targets: dict[str, str] = {}
            markers: dict[str, str] = {}

            for column in self._columns:
                if column.target == table_target:
                    importances[column.table] += column.importance
                    targets[column.table] = column.target
                    markers[column.table] = column.marker

            tables_zip = zip(
                importances.keys(),
                importances.values(),
                targets.values(),
                markers.values(),
            )

            for name, importance, target, marker in tables_zip:
                tables.append(
                    Table(
                        name=name, importance=importance, target=target, marker=marker
                    )
                )

        self.data = tables

    # ----------------------------------------------------------------

    def _make_tables(self, data: Sequence[Table]) -> Tables:
        """
        A factory to construct a :class:`getml.pipeline.Tables` container
        from a list of :class:`getml.pipeline.Table`s.
        """

        return Tables(self._targets, self._columns, data=data)

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable[[Table], bool]) -> Tables:
        """
        Filters the tables container.

        Args:
            conditional (callable, optional):
                A callable that evaluates to a boolean for a given item.

        Return:
            :class:`getml.pipeline.Tables`:
                A container of filtered tables.

        Example:
            .. code-block:: python

                important_tables = my_pipeline.table.filter(
                    lambda table: table.importance > 0.1)

                peripheral_tables = my_pipeline.tables.filter(
                    lambda table: table.marker == "[PERIPHERAL]")

        """
        tables_filtered = [table for table in self.data if conditional(table)]
        return self._make_tables(tables_filtered)

    # ----------------------------------------------------------------

    def importances(
        self, target_num: int = 0, sort: bool = True
    ) -> Tuple[NDArray[np.str_], NDArray[np.float_]]:
        """
        Returns the importances of tables.

        Table importances are calculated by summing up the importances of the
        columns belonging to the tables. Each column is assigned an importance
        value that measures its contribution to the predictive performance. For
        each target, the importances add up to 1.

        Args:
            target_num (int):
                Indicates for which target you want to view the
                importances. (Pipelines can have more than one target.)

            sort (bool):
                Whether you want the results to be sorted.

        Return:
            (:class:`numpy.ndarray`, :class:`numpy.ndarray`):
                - The first array contains the names of the tables.

                - The second array contains their importances. By definition,
                  all importances add up to 1.
        """

        target_name = self._targets[target_num]

        names = np.empty(0, dtype=str)
        importances = np.empty(0, dtype=float)

        for table in self.data:
            if table.target == target_name:
                names = np.append(names, table.name)
                importances = np.append(importances, table.importance)

        if not sort:
            return names, importances

        indices = np.argsort(importances)[::-1]

        return (names[indices], importances[indices])

    # ----------------------------------------------------------------

    @property
    def names(self) -> list[str]:
        """
        Holds the names of a :class:`~getml.Pipeline`\'s tables.

        Returns:
            :class:`list` containing the names.

        Note:
            The order corresponds to the current sorting of the container.
        """
        return [table.name for table in self.data]

    # ----------------------------------------------------------------

    def sort(
        self,
        by: Optional[str] = None,
        key: Optional[Callable[[Table], Any]] = None,
        descending: Optional[bool] = None,
    ) -> Tables:
        """
        Sorts the Tables container. If no arguments are provided the
        container is sorted by target and name.

        Args:
            by (str, optional):
                The name of field to sort by. Possible fields:
                    - name(s)
                    - importances(s)
            key (callable, optional):
                A callable that evaluates to a sort key for a given item.
            descending (bool, optional):
                Whether to sort in descending order.

        Return:
            :class:`getml.pipeline.Tables`:
                A container of sorted tables.

        Example:
            .. code-block:: python

                by_importance = my_pipeline.tables.sort(key=lambda table: table.importance)

        """

        reverse = False if descending is None else descending

        if (by is not None) and (key is not None):
            raise ValueError("Only one of `by` and `key` can be provided.")

        if key is not None:
            tables_sorted = sorted(self.data, key=key, reverse=reverse)
            return self._make_tables(tables_sorted)

        if by is None:
            tables_sorted = sorted(
                self.data, key=lambda table: table.name, reverse=reverse
            )
            tables_sorted.sort(key=lambda table: table.target)
            return self._make_tables(tables_sorted)

        if re.match(pattern="names?$", string=by):
            tables_sorted = sorted(
                self.data, key=lambda table: table.name, reverse=reverse
            )
            return self._make_tables(tables_sorted)

        if re.match(pattern="importances?$", string=by):
            reverse = True if descending is None else descending
            tables_sorted = sorted(
                self.data, key=lambda table: table.importance, reverse=reverse
            )
            return self._make_tables(tables_sorted)

        raise ValueError(f"Cannot sort by: {by}.")

    # ----------------------------------------------------------------

    @property
    def targets(self) -> list[str]:
        """
        Holds the targets of a :class:`~getml.Pipeline`\'s tables.

        Returns:
            :class:`list` containing the names.

        Note:
            The order corresponds to the current sorting of the container.
        """
        return [table.target for table in self.data]

    # ----------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """
        Returns all information related to the tables in a pandas DataFrame.
        """

        data_frame = pd.DataFrame()

        for i, table in enumerate(self.data):
            data_frame.loc[i, "name"] = table.name
            data_frame.loc[i, "importance"] = table.importance
            data_frame.loc[i, "target"] = table.target
            data_frame.loc[i, "marker"] = table.marker

        return data_frame
