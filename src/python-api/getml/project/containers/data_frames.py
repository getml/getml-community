# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Container for data frames in memory.
"""

from __future__ import annotations

from typing import Callable, List

from getml.data import DataFrame
from getml.data.helpers import list_data_frames
from getml.data.helpers2 import load_data_frame
from getml.utilities.formatting import _Formatter

# --------------------------------------------------------------------


class DataFrames:
    """
    Container which holds all data frames associated with the running
    project that are currently stored in memory. The container supports
    slicing and is sort- and filterable.
    """

    # ----------------------------------------------------------------

    def __init__(self, data=None):
        self._in_memory = list_data_frames()["in_memory"]
        self._on_disk = list_data_frames()["on_disk"]

        if data is None:
            self.data = [load_data_frame(name) for name in self._in_memory]
        else:
            self.data = data

    # ----------------------------------------------------------------

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            dfs_subset = self.data[key]
            return DataFrames(data=dfs_subset)
        if isinstance(key, str):
            if key in self.in_memory:
                return [df for df in self.data if df.name == key][0]
            if key in self.on_disk:
                raise AttributeError(f"DataFrame {key} not loaded from disk.")
            raise AttributeError(f"No DataFrame with name: {key}")
        raise TypeError(
            f"DataFrames can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ----------------------------------------------------------------

    def __repr__(self):
        if len(self.in_memory) == 0:
            output = "No data frames in memory."
        else:
            output = self._format()._render_string()

        if len(self.on_disk) > 0:
            output += "\n\nOn disk:\n"
            output += "\n".join(self.on_disk)

        return output

    # ----------------------------------------------------------------

    def _repr_html_(self):
        if len(self.in_memory) == 0:
            output = "<p>No data frames in memory.</p>"
        else:
            output = self._format()._render_html()

        if len(self.on_disk) > 0:
            output += "<p>On disk:</p>"
            output += "<br>".join(self.on_disk)

        return output

    # ----------------------------------------------------------------

    def _format(self):
        headers = [["name", "rows", "columns", "memory usage"]]

        rows = [[df.name, df.nrows(), df.ncols(), df.memory_usage] for df in self.data]

        formatted = _Formatter(headers, rows)

        formatted[4].cell_template = "{:{width}.2f} MB"

        return formatted

    # ----------------------------------------------------------------

    def delete(self) -> None:
        """
        Deletes all data frames in the current project.
        """

        for name in self.on_disk:
            DataFrame(name).delete()

    # ----------------------------------------------------------------

    @property
    def in_memory(self) -> List[str]:
        """
        Returns the names of all data frames currently in memory.

        Returns:
                The names of all data frames currently in memory.
        """
        return self._in_memory

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable) -> DataFrames:
        """
        Filters the data frames container.

        Args:
            conditional:
                A callable that evaluates to a boolean for a given item.

        Returns:
                A container of filtered data frames.

        ??? example
            ```python
            big_frames = getml.project.data_frames.filter(lambda frame: frame.memory_usage > 1000)
            ```
        """

        dfs_filtered = [df for df in self.data if conditional(df)]
        return DataFrames(data=dfs_filtered)

    # ----------------------------------------------------------------

    def load(self) -> None:
        """
        Loads all data frames stored in the project folder to memory.
        """

        for df in self.on_disk:
            if df not in self.in_memory:
                self.data.append(load_data_frame(df))

    # ----------------------------------------------------------------

    @property
    def on_disk(self) -> List[str]:
        """
        Returns the names of all data frames stored in the project folder.
        """
        return self._on_disk

    # ----------------------------------------------------------------

    def retrieve(self):
        """
        Retrieve a dict of all data frames in memory.
        """

        return {df.name: df for df in self.data}

    # ----------------------------------------------------------------

    def save(self) -> None:
        """
        Saves all data frames currently in memory to disk.
        """

        for df in self.data:
            df.save()

    # ----------------------------------------------------------------

    def sort(self, key: Callable, descending: bool = False) -> DataFrames:
        """
        Sorts the data frames container.

        Args:
            key:
                A callable that evaluates to a sort key for a given item.

            descending:
                Whether to sort in descending order.

        Returns:
                A container of sorted data frames.

        ??? example
            ```python
            by_num_rows = getml.project.data_frames.sort(lambda frame: frame.nrows())
            ```
        """

        dfs_sorted = sorted(self.data, key=key, reverse=descending)
        return DataFrames(data=dfs_sorted)

    # ----------------------------------------------------------------

    def unload(self) -> None:
        """
        Unloads all data frames in the current project from memory.
        """

        for name in self.on_disk:
            DataFrame(name).unload()
