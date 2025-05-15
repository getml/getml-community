# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""Handler for the data stored in the getML Engine."""

from __future__ import annotations

import json
import numbers
import os
import shutil
import warnings
from collections import namedtuple
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Hashable,
    Iterable,
    Iterator,
    List,
    Literal,
    Optional,
    Union,
    cast,
    overload,
)

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import getml.communication as comm
from getml import constants, database
from getml.constants import COMPARISON_ONLY, DEFAULT_BATCH_SIZE, TIME_STAMP
from getml.data import roles as roles_
from getml.data._io.arrow import (
    cast_arrow_batch,
    preprocess_arrow_schema,
    read_arrow_batches,
    sniff_schema,
    to_arrow,
    to_arrow_batches,
    to_arrow_stream,
)
from getml.data._io.csv import (
    DEFAULT_CSV_READ_BLOCK_SIZE,
    read_csv,
    sniff_csv,
    stream_csv,
    to_csv,
)
from getml.data._io.parquet import sniff_parquet, to_parquet
from getml.data.columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    rowid,
)
from getml.data.columns.last_change import _last_change
from getml.data.helpers import (
    _check_if_exists,
    _empty_data_frame,
    _exists_in_memory,
    _get_column,
    _handle_cols,
    _is_non_empty_typed_list,
    _is_numerical_type_numpy,
    _iter_batches,
    _prepare_roles,
    _send_numpy_array,
    _sniff_db,
    _sniff_query,
    _sniff_s3,
    _to_pyspark,
    _where,
    _with_column,
    _with_role,
    _with_subroles,
    _with_unit,
)
from getml.data.placeholder import Placeholder
from getml.data.roles import sets as roles_sets
from getml.data.roles.container import Roles
from getml.data.roles.types import Role
from getml.data.subroles.types import Subrole
from getml.data.view import View
from getml.database import Connection
from getml.database.helpers import _retrieve_temp_dir, _retrieve_urls
from getml.helpers import _is_iterable_not_str, _is_iterable_not_str_of_type
from getml.utilities.formatting import _DataFrameFormatter

if TYPE_CHECKING:
    import pyspark.sql

    from getml.data.data_frame import DataFrame
    from getml.data.view import View

# --------------------------------------------------------------------


class DataFrame:
    """Handler for the data stored in the getML Engine.

    The [`DataFrame`][getml.DataFrame] class represents a data frame
    object in the getML Engine but does not contain any actual data
    itself. To create such a data frame object, fill it with data via
    the Python API, and to retrieve a handler for it, you can use one
    of the [`from_csv`][getml.DataFrame.from_csv],
    [`from_db`][getml.DataFrame.from_db],
    [`from_json`][getml.DataFrame.from_json], or
    [`from_pandas`][getml.DataFrame.from_pandas] class methods. The
    [Importing Data][importing-data] section in the user guide explains the
    particularities of each of those flavors of the unified
    import interface.

    If the data frame object is already present in the Engine -
    either in memory as a temporary object or on disk when
    [`save`][getml.DataFrame.save] was called earlier -, the
    [`load_data_frame`][getml.data.load_data_frame] function will create a new
    handler without altering the underlying data. For more information
    about the lifecycle of the data in the getML Engine and its
    synchronization with the Python API please see the
    corresponding [User Guide][python-api-lifecycles].

    Attributes:
        name:
            Unique identifier used to link the handler with
            the underlying data frame object in the Engine.

        roles:
            Maps the [`roles`][getml.data.roles] to the
            column names (see [`colnames`][getml.DataFrame.colnames]).

            The `roles` dictionary is expected to have the following format
            ```python
            roles = {getml.data.role.numeric: ["colname1", "colname2"],
                     getml.data.role.target: ["colname3"]}
            ```
            Otherwise, you can use the [`Roles`][getml.data.Roles] class.

    ??? example
        Creating a new data frame object in the getML Engine and importing
        data is done by one the class functions
        [`from_csv`][getml.DataFrame.from_csv],
        [`from_db`][getml.DataFrame.from_db],
        [`from_json`][getml.DataFrame.from_json], or
        [`from_pandas`][getml.DataFrame.from_pandas].

        ```python
        random = numpy.random.RandomState(7263)

        table = pandas.DataFrame()
        table['column_01'] = random.randint(0, 10, 1000).astype(numpy.str)
        table['join_key'] = numpy.arange(1000)
        table['time_stamp'] = random.rand(1000)
        table['target'] = random.rand(1000)

        df_table = getml.DataFrame.from_pandas(table, name = 'table')
        ```
        In addition to creating a new data frame object in the getML
        Engine and filling it with all the content of `table`, the
        [`from_pandas`][getml.DataFrame.from_pandas] function also
        returns a [`DataFrame`][getml.DataFrame] handler to the
        underlying data.

        You don't have to create the data frame objects anew for each
        session. You can use their [`save`][getml.DataFrame.save]
        method to write them to disk, the
        [`list_data_frames`][getml.data.list_data_frames] function to list all
        available objects in the Engine, and
        [`load_data_frame`][getml.data.load_data_frame] to create a
        [`DataFrame`][getml.DataFrame] handler for a data set already
        present in the getML Engine (see
        [User Guide][python-api] for details).

        ```python
        df_table.save()

        getml.data.list_data_frames()

        df_table_reloaded = getml.data.load_data_frame('table')
        ```

    Note:
        Although the Python API does not store the actual data itself,
        you can use the [`to_csv`][getml.DataFrame.to_csv],
        [`to_db`][getml.DataFrame.to_db],
        [`to_json`][getml.DataFrame.to_json], and
        [`to_pandas`][getml.DataFrame.to_pandas] methods to retrieve
        them.

    """

    _categorical_roles = roles_sets.categorical
    _numerical_roles = roles_sets.numerical
    _all_roles = roles_sets.all_

    def __init__(
        self,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
    ):
        # ------------------------------------------------------------

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        vars(self)["name"] = name

        if roles is None:
            roles = {}

        if isinstance(roles, dict):
            roles = Roles.from_dict(roles)

        # ------------------------------------------------------------

        vars(self)["_categorical_columns"] = [
            StringColumn(name=cname, role=roles_.categorical, df_name=self.name)
            for cname in roles.categorical
        ]

        vars(self)["_join_key_columns"] = [
            StringColumn(name=cname, role=roles_.join_key, df_name=self.name)
            for cname in roles.join_key
        ]

        vars(self)["_numerical_columns"] = [
            FloatColumn(name=cname, role=roles_.numerical, df_name=self.name)
            for cname in roles.numerical
        ]

        vars(self)["_target_columns"] = [
            FloatColumn(name=cname, role=roles_.target, df_name=self.name)
            for cname in roles.target
        ]

        vars(self)["_text_columns"] = [
            StringColumn(name=cname, role=roles_.text, df_name=self.name)
            for cname in roles.text
        ]

        vars(self)["_time_stamp_columns"] = [
            FloatColumn(name=cname, role=roles_.time_stamp, df_name=self.name)
            for cname in roles.time_stamp
        ]

        vars(self)["_unused_float_columns"] = [
            FloatColumn(name=cname, role=roles_.unused_float, df_name=self.name)
            for cname in roles.unused_float
        ]

        vars(self)["_unused_string_columns"] = [
            StringColumn(name=cname, role=roles_.unused_string, df_name=self.name)
            for cname in roles.unused_string
        ]

        # ------------------------------------------------------------

        self._check_duplicates()

    # ----------------------------------------------------------------

    @property
    def _columns(self):
        return (
            vars(self)["_categorical_columns"]
            + vars(self)["_join_key_columns"]
            + vars(self)["_numerical_columns"]
            + vars(self)["_target_columns"]
            + vars(self)["_text_columns"]
            + vars(self)["_time_stamp_columns"]
            + vars(self)["_unused_float_columns"]
            + vars(self)["_unused_string_columns"]
        )

    # ----------------------------------------------------------------

    def _delete(self, mem_only: bool = False):
        """Deletes the data frame from the getML Engine.

        If called with the `mem_only` option set to True, the data
        frame corresponding to the handler represented by the current
        instance can be reloaded using the
        [`load`][getml.DataFrame.load] method.

        Args:
            mem_only:
                If True, the data frame will not be deleted
                permanently, but just from memory (RAM).
        """

        if not isinstance(mem_only, bool):
            raise TypeError("'mem_only' must be of type bool")

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.delete"
        cmd["name_"] = self.name
        cmd["mem_only_"] = mem_only

        comm.send(cmd)

    # ------------------------------------------------------------

    def __delitem__(self, colname: str):
        self._drop(colname)

    # ----------------------------------------------------------------

    def __eq__(self, other):
        if not isinstance(other, DataFrame):
            raise TypeError(
                "A DataFrame can only be compared to another getml.DataFrame"
            )

        # ------------------------------------------------------------

        for kkey in self.__dict__:
            if kkey not in other.__dict__:
                return False

            # Take special care when comparing numbers.
            if isinstance(self.__dict__[kkey], numbers.Real):
                if not np.isclose(self.__dict__[kkey], other.__dict__[kkey]):
                    return False

            elif self.__dict__[kkey] != other.__dict__[kkey]:
                return False

        # ------------------------------------------------------------

        return True

    # ----------------------------------------------------------------

    def __getattr__(self, name: str) -> Union[FloatColumn, StringColumn]:
        try:
            return self[name]
        except KeyError:
            return super().__getattribute__(name)

    # ------------------------------------------------------------

    @overload
    def __getitem__(self, name: str) -> Union[FloatColumn, StringColumn]: ...

    @overload
    def __getitem__(
        self,
        name: Union[
            Iterable[str],
            numbers.Real,
            slice,
            BooleanColumnView,
            FloatColumn,
            FloatColumnView,
        ],
    ) -> View: ...

    def __getitem__(
        self,
        name: Union[
            str,
            Iterable[str],
            numbers.Real,
            slice,
            BooleanColumnView,
            FloatColumn,
            FloatColumnView,
        ],
    ) -> Union[FloatColumn, StringColumn, View]:
        if isinstance(
            name,
            (numbers.Integral, slice, BooleanColumnView, FloatColumn, FloatColumnView),
        ):
            return self.where(index=name)

        if _is_iterable_not_str(name):
            not_in_colnames = set(name) - set(self.colnames)
            if not_in_colnames:
                raise KeyError(f"{list(not_in_colnames)} not found.")
            dropped = [col for col in self.colnames if col not in name]
            return View(base=self, dropped=dropped)

        col = _get_column(name, self._columns)

        if col is not None:
            return col

        raise KeyError(f"Column named '{name}' not found.")

    # ------------------------------------------------------------

    def _getml_deserialize(self) -> Dict[str, Any]:
        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame"
        cmd["name_"] = self.name

        return cmd

    # ------------------------------------------------------------

    def _dir(self) -> List[str]:
        return sorted(set(dir(type(self)) + list(self.__dict__) + self.colnames))

    # ----------------------------------------------------------------

    def __len__(self) -> int:
        return self.nrows()

    # ----------------------------------------------------------------

    def __repr__(self) -> str:
        if not _exists_in_memory(self.name):
            return _empty_data_frame()

        formatted = self._format()

        footer = self._collect_footer_data()

        return formatted._render_string(footer=footer)

    # ----------------------------------------------------------------

    def _repr_html_(self) -> str:
        return self.to_html()

    # ------------------------------------------------------------

    def __setattr__(self, name, value) -> None:
        if name in vars(self):
            vars(self)[name] = value
        else:
            self.add(value, name)

    # ------------------------------------------------------------

    def __setitem__(self, name, col) -> None:
        self.add(col, name)

    # ------------------------------------------------------------

    def _add_categorical_column(self, col, name, role, subroles, unit):
        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.add_categorical_column"
        cmd["name_"] = name

        cmd["col_"] = col.cmd
        cmd["df_name_"] = self.name
        cmd["role_"] = role
        cmd["subroles_"] = subroles
        cmd["unit_"] = unit

        with comm.send_and_get_socket(cmd) as sock:
            comm.recv_issues(sock)
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.handle_engine_exception(msg)

        self.refresh()

    # ------------------------------------------------------------

    def _add_column(self, col, name, role, subroles, unit):
        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.add_column"
        cmd["name_"] = name

        cmd["col_"] = col.cmd
        cmd["df_name_"] = self.name
        cmd["role_"] = role
        cmd["subroles_"] = subroles
        cmd["unit_"] = unit

        with comm.send_and_get_socket(cmd) as sock:
            comm.recv_issues(sock)
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.handle_engine_exception(msg)

        self.refresh()

    # ------------------------------------------------------------

    def _add_numpy_array(self, numpy_array, name, role, subroles, unit):
        if len(numpy_array.shape) != 1:
            raise TypeError(
                """numpy.ndarray needs to be one-dimensional!
                Maybe you can call .ravel()."""
            )

        if role is None:
            temp_df = pd.DataFrame()
            temp_df["column"] = numpy_array
            if _is_numerical_type_numpy(temp_df.dtypes.iloc[0]):
                role = roles_.unused_float
            else:
                role = roles_.unused_string

        col = (
            FloatColumn(name=name, role=role, df_name=self.name)
            if role in self._numerical_roles
            else StringColumn(name=name, role=role, df_name=self.name)
        )

        _send_numpy_array(col, numpy_array)

        if subroles:
            self._set_subroles(name, append=False, subroles=subroles)

        if unit:
            self._set_unit(name, unit)

        self.refresh()

    # ------------------------------------------------------------

    def _check_duplicates(self) -> None:
        all_colnames: List[str] = []

        all_colnames = _check_if_exists(self._categorical_names, all_colnames)

        all_colnames = _check_if_exists(self._join_key_names, all_colnames)

        all_colnames = _check_if_exists(self._numerical_names, all_colnames)

        all_colnames = _check_if_exists(self._target_names, all_colnames)

        all_colnames = _check_if_exists(self._text_names, all_colnames)

        all_colnames = _check_if_exists(self._time_stamp_names, all_colnames)

        all_colnames = _check_if_exists(self._unused_names, all_colnames)

    # ------------------------------------------------------------

    def _check_plausibility(self, data_frame):
        self._check_duplicates()

        for col in self._categorical_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._join_key_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._numerical_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._target_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._text_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._time_stamp_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

        for col in self._unused_names:
            if col not in data_frame.columns:
                raise ValueError("Column named '" + col + "' does not exist!")

    # ------------------------------------------------------------

    def _collect_footer_data(self):
        footer = namedtuple(
            "footer", ["n_rows", "n_cols", "memory_usage", "name", "type", "url"]
        )

        return footer(
            n_rows=self.nrows(),
            n_cols=self.ncols(),
            memory_usage=self.memory_usage,
            name=self.name,
            type="getml.DataFrame",
            url=self._monitor_url,
        )

    # ------------------------------------------------------------

    def _close(self, sock):
        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.close"
        cmd["name_"] = self.name

        comm.send_string(sock, json.dumps(cmd))

        msg = comm.recv_string(sock)

        if msg != "Success!":
            comm.handle_engine_exception(msg)

    # ------------------------------------------------------------

    def _drop(self, colname: str):
        if not isinstance(colname, str):
            raise TypeError("'colname' must be either a string.")

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.remove_column"
        cmd["name_"] = colname

        cmd["df_name_"] = self.name

        comm.send(cmd)

        self.refresh()

    # ------------------------------------------------------------

    def _format(self):
        formatted = _DataFrameFormatter(self)
        return formatted

    # ------------------------------------------------------------

    def _set_role(self, name, role, time_formats):
        if not isinstance(name, str):
            raise TypeError("Parameter 'name' must be a string!")

        col = self[name]

        subroles = col.subroles

        unit = TIME_STAMP + COMPARISON_ONLY if role == roles_.time_stamp else col.unit

        self.add(
            col,
            name=name,
            role=role,
            subroles=subroles,
            unit=unit,
            time_formats=time_formats,
        )

    # ------------------------------------------------------------

    def _set_subroles(
        self,
        name: str,
        append: bool,
        subroles: Optional[Union[Subrole, Iterable[str]]] = None,
    ):
        if not isinstance(name, str):
            raise TypeError("Parameter 'name' must be a string!")

        col = self[name]

        cmd: Dict[str, Any] = {}

        cmd.update(col.cmd)

        cmd["type_"] += ".set_subroles"

        cmd["subroles_"] = list(set(col.subroles + subroles)) if append else subroles

        comm.send(cmd)

    # ------------------------------------------------------------

    def _set_unit(self, name: str, unit: str):
        if not isinstance(name, str):
            raise TypeError("Parameter 'name' must be a string!")

        col = self[name]

        cmd: Dict[str, Any] = {}

        cmd.update(col.cmd)

        cmd["type_"] += ".set_unit"

        cmd["unit_"] = unit

        comm.send(cmd)

    # ------------------------------------------------------------

    def add(
        self,
        col: Union[StringColumn, FloatColumn, np.ndarray],
        name: str,
        role: Optional[Role] = None,
        subroles: Optional[Union[Role, Iterable[str]]] = None,
        unit: str = "",
        time_formats: Optional[Iterable[str]] = None,
    ):
        """Adds a column to the current [`DataFrame`][getml.DataFrame].

        Args:
            col:
                The column or numpy.ndarray to be added.

            name:
                Name of the new column.

            role:
                Role of the new column. Must be from [`roles`][getml.data.roles].

            subroles:
                Subroles of the new column. Must be from [`subroles`][getml.data.subroles].

            unit:
                Unit of the column.

            time_formats:
                Formats to be used to parse the time stamps.

                This is only necessary, if an implicit conversion from
                a [`StringColumn`][getml.data.columns.StringColumn] to a time
                stamp is taking place.

                The formats are allowed to contain the following
                special characters:

                * %w - abbreviated weekday (Mon, Tue, ...)
                * %W - full weekday (Monday, Tuesday, ...)
                * %b - abbreviated month (Jan, Feb, ...)
                * %B - full month (January, February, ...)
                * %d - zero-padded day of month (01 .. 31)
                * %e - day of month (1 .. 31)
                * %f - space-padded day of month ( 1 .. 31)
                * %m - zero-padded month (01 .. 12)
                * %n - month (1 .. 12)
                * %o - space-padded month ( 1 .. 12)
                * %y - year without century (70)
                * %Y - year with century (1970)
                * %H - hour (00 .. 23)
                * %h - hour (00 .. 12)
                * %a - am/pm
                * %A - AM/PM
                * %M - minute (00 .. 59)
                * %S - second (00 .. 59)
                * %s - seconds and microseconds (equivalent to %S.%F)
                * %i - millisecond (000 .. 999)
                * %c - centisecond (0 .. 9)
                * %F - fractional seconds/microseconds (000000 - 999999)
                * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
                * %Z - time zone differential in RFC format (GMT or +NNNN)
                * %% - percent sign
        """

        if isinstance(col, np.ndarray):
            self._add_numpy_array(col, name, role, subroles, unit)
            return

        col, role, subroles = _with_column(
            col, name, role, subroles, unit, time_formats
        )

        is_string = isinstance(col, (StringColumnView, StringColumn))

        if is_string:
            self._add_categorical_column(col, name, role, subroles, unit)
        else:
            self._add_column(col, name, role, subroles, unit)

    # ------------------------------------------------------------

    @property
    def _categorical_names(self) -> List[str]:
        return [col.name for col in self._categorical_columns]

    # ------------------------------------------------------------

    @property
    def colnames(self) -> List[str]:
        """
        List of the names of all columns.

        Returns:
                List of the names of all columns.
        """
        return (
            self._time_stamp_names
            + self._join_key_names
            + self._target_names
            + self._categorical_names
            + self._numerical_names
            + self._text_names
            + self._unused_names
        )

    # ------------------------------------------------------------

    @property
    def columns(self) -> List[str]:
        """
        Alias for [`colnames`][getml.DataFrame.colnames].

        Returns:
                List of the names of all columns.
        """
        return self.colnames

    # ------------------------------------------------------------

    def copy(self, name: str) -> DataFrame:
        """
        Creates a deep copy of the data frame under a new name.

        Args:
            name:
                The name of the new data frame.

        Returns:
                A handle to the deep copy.
        """

        if not isinstance(name, str):
            raise TypeError("'name' must be a string.")

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.concat"
        cmd["name_"] = name

        cmd["data_frames_"] = [self._getml_deserialize()]

        comm.send(cmd)

        return DataFrame(name=name).refresh()

    # ------------------------------------------------------------

    def delete(self):
        """
        Permanently deletes the data frame. `delete` first unloads the data frame
        from memory and then deletes it from disk.
        """
        # ------------------------------------------------------------

        self._delete()

    # ------------------------------------------------------------

    def drop(
        self,
        cols: Union[
            FloatColumn,
            StringColumn,
            str,
            Union[Iterable[FloatColumn], Iterable[StringColumn], Iterable[str]],
        ],
    ) -> View:
        """Returns a new [`View`][getml.data.View] that has one or several columns removed.

        Args:
            cols:
                The columns or the names thereof.

        Returns:
            A new [`View`][getml.data.View] object with the specified columns removed.
        """

        names = _handle_cols(cols)

        return View(base=self, dropped=names)

    # ------------------------------------------------------------

    def freeze(self):
        """Freezes the data frame.

        After you have frozen the data frame, the data frame is immutable
        and in-place operations are no longer possible. However, you can
        still create views. In other words, operations like
        [`set_role`][getml.DataFrame.set_role] are no longer possible,
        but operations like [`with_role`][getml.DataFrame.with_role] are.
        """
        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.freeze"
        cmd["name_"] = self.name
        comm.send(cmd)

    # ------------------------------------------------------------

    @overload
    @classmethod
    def from_arrow(
        cls,
        table: pa.Table,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_arrow(
        cls,
        table: pa.Table,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_arrow(
        cls,
        table: pa.Table,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from an Arrow Table.

        This is one of the fastest way to get data into the
        getML Engine.

        Args:
            table:
                The arrow tablelike to be read.

            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
                Handler of the underlying data.
        """

        # ------------------------------------------------------------

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # ------------------------------------------------------------

        sniffed_roles = sniff_schema(table.schema)

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_arrow(table=table, append=False)

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_csv(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        num_lines_sniffed: None = None,
        num_lines_read: int = 0,
        quotechar: str = '"',
        sep: str = ",",
        skip: int = 0,
        colnames: Iterable[str] = (),
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
        verbose: bool = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_csv(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        num_lines_sniffed: None = None,
        num_lines_read: int = 0,
        quotechar: str = '"',
        sep: str = ",",
        skip: int = 0,
        colnames: Iterable[str] = (),
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
        verbose: bool = False,
    ) -> Roles: ...

    @classmethod
    def from_csv(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        num_lines_sniffed: None = None,
        num_lines_read: int = 0,
        quotechar: str = '"',
        sep: str = ",",
        skip: int = 0,
        colnames: Iterable[str] = (),
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
        verbose: bool = True,
        block_size: int = DEFAULT_CSV_READ_BLOCK_SIZE,
        in_batches: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from CSV files.

        The getML Engine will construct a data
        frame object in the Engine, fill it with the data read from
        the CSV file(s), and return a corresponding
        [`DataFrame`][getml.DataFrame] handle.

        Args:
            fnames:
                CSV file paths to be read.

            name:
                Name of the data frame to be created.

            num_lines_sniffed:
                Number of lines analyzed by the sniffer.

            num_lines_read:
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            quotechar:
                The character used to wrap strings.

            sep:
                The separator used for separating fields.

            skip:
                Number of lines to skip at the beginning of each file.

            colnames: The first line of a CSV file
                usually contains the column names. When this is not the case,
                you need to explicitly pass them.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

            verbose:
                If True, when fnames are urls, the filenames are
                printed to stdout during the download.

            block_size:
                The number of bytes read with each batch. Passed down to
                pyarrow.

            in_batches:
                If True, read blocks streamwise manner and send those batches to
                the engine. Blocks are read and sent to the engine sequentially.
                While more memory efficient, streaming in batches is slower as
                it is inherently single-threaded. If False (default) the data is
                read with multiple threads into arrow first and sent to the engine
                afterwards.

        Returns:
                Handler of the underlying data.


        Deprecated:
            1.5: The `num_lines_sniffed` parameter is deprecated.

        Note:
            It is assumed that the first line of each CSV file
            contains a header with the column names.

        ??? example
            Let's assume you have two CSV files - *file1.csv* and
            *file2.csv* - in the current working directory. You can
            import their data into the getML Engine using.
            ```python
            df_expd = data.DataFrame.from_csv(
                fnames=["file1.csv", "file2.csv"],
                name="MY DATA FRAME",
                sep=';',
                quotechar='"'
                )

            # However, the CSV format lacks type safety. If you want to
            # build a reliable pipeline, it is a good idea
            # to hard-code the roles:

            roles = {"categorical": ["col1", "col2"], "target": ["col3"]}

            df_expd = data.DataFrame.from_csv(
                fnames=["file1.csv", "file2.csv"],
                name="MY DATA FRAME",
                sep=';',
                quotechar='"',
                roles=roles
                )

            # If you think that typing out all the roles by hand is too
            # cumbersome, you can use a dry run:

            roles = data.DataFrame.from_csv(
                fnames=["file1.csv", "file2.csv"],
                name="MY DATA FRAME",
                sep=';',
                quotechar='"',
                dry=True
            )
            ```

            This will return the roles dictionary it would have used. You
            can now hard-code this.

        """

        if num_lines_sniffed is not None:
            warnings.warn(
                "The 'num_lines_sniffed' parameter is deprecated and will be ignored.",
                DeprecationWarning,
            )

        if isinstance(fnames, str):
            fnames = [fnames]

        if not _is_non_empty_typed_list(fnames, str):
            raise TypeError("'fnames' must be either a str or a list of str.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        if not isinstance(num_lines_read, numbers.Real):
            raise TypeError("'num_lines_read' must be a real number")

        if not isinstance(quotechar, str):
            raise TypeError("'quotechar' must be str.")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be str.")

        if not isinstance(skip, numbers.Real):
            raise TypeError("'skip' must be a real number")

        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(ignore, bool):
            raise TypeError("'dry' must be bool.")

        if colnames:
            if not _is_iterable_not_str_of_type(colnames, str):
                raise TypeError("'colnames' must be an iterable of str")

        fnames = _retrieve_urls(fnames, verbose=verbose)

        sniffed_roles = sniff_csv(
            fnames=fnames,
            quotechar=quotechar,
            sep=sep,
            skip=int(skip),
            colnames=colnames,
        )

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_csv(
            fnames=fnames,
            append=False,
            quotechar=quotechar,
            sep=sep,
            num_lines_read=num_lines_read,
            skip=skip,
            colnames=colnames,
            block_size=block_size,
            in_batches=in_batches,
        )

    # ------------------------------------------------------------

    @overload
    @classmethod
    def from_db(
        cls,
        table_name: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
        conn: Optional[Connection] = None,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_db(
        cls,
        table_name: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
        conn: Optional[Connection] = None,
    ) -> Roles: ...

    @classmethod
    def from_db(
        cls,
        table_name: str,
        name: Optional[str] = None,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
        conn: Optional[Connection] = None,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from a table in a database.

        It will construct a data frame object in the Engine, fill it
        with the data read from table `table_name` in the connected
        database (see [`database`][getml.database]), and return a
        corresponding [`DataFrame`][getml.DataFrame] handle.

        Args:
            table_name:
                Name of the table to be read.

            name:
                Name of the data frame to be created. If not passed,
                then the *table_name* will be used.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

            conn:
                The database connection to be used.
                If you don't explicitly pass a connection, the Engine
                will use the default connection.

        Returns:
                Handler of the underlying data.

        ??? example
            ```python
            getml.database.connect_mysql(
                host="relational.fel.cvut.cz",
                port=3306,
                dbname="financial",
                user="guest",
                password="ctu-relational"
            )

            loan = getml.DataFrame.from_db(
                table_name='loan', name='data_frame_loan')
            ```
        """

        # -------------------------------------------

        name = name or table_name

        # -------------------------------------------

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be str.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError(
                "'roles' must be a getml.data.Roles object, a dict or None."
            )

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # -------------------------------------------

        conn = conn or database.Connection()

        # ------------------------------------------------------------

        sniffed_roles = _sniff_db(table_name, conn)

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        # ------------------------------------------------------------

        data_frame = cls(name, roles)

        return data_frame.read_db(table_name=table_name, append=False, conn=conn)

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_dict(
        cls,
        data: Dict[Hashable, Iterable[Any]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_dict(
        cls,
        data: Dict[Hashable, Iterable[Any]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_dict(
        cls,
        data: Dict[Hashable, Iterable[Any]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a new DataFrame from a dict

        Args:
            data:
                The dict containing the data.
                The data should be in the following format:
                ```python
                data = {'col1': [1.0, 2.0, 1.0], 'col2': ['A', 'B', 'C']}
                ```
            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.
        Returns:
                Handler of the underlying data.
        """

        if not isinstance(data, dict):
            raise TypeError("'data' must be dict.")

        return cls.from_arrow(
            table=pa.Table.from_pydict(data),
            name=name,
            roles=roles,
            ignore=ignore,
            dry=dry,
        )

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_json(
        cls,
        json_str: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_json(
        cls,
        json_str: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_json(
        cls,
        json_str: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a new DataFrame from a JSON string.

        It will construct a data frame object in the Engine, fill it
        with the data read from the JSON string, and return a
        corresponding [`DataFrame`][getml.DataFrame] handle.

        Args:
            json_str:
                The JSON string containing the data.
                The json_str should be in the following format:
                ```python
                json_str = "{'col1': [1.0, 2.0, 1.0], 'col2': ['A', 'B', 'C']}"
                ```
            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
            Handler of the underlying data.

        """

        if not isinstance(json_str, str):
            raise TypeError("'json_str' must be str.")

        return cls.from_dict(
            data=json.loads(json_str),
            name=name,
            roles=roles,
            ignore=ignore,
            dry=dry,
        )

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_pandas(
        cls,
        pandas_df: pd.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_pandas(
        cls,
        pandas_df: pd.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_pandas(
        cls,
        pandas_df: pd.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from a `pandas.DataFrame`.

        It will construct a data frame object in the Engine, fill it
        with the data read from the `pandas.DataFrame`, and
        return a corresponding [`DataFrame`][getml.DataFrame] handle.

        Args:
            pandas_df:
                The table to be read.

            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                 roles = {getml.data.role.numeric: ["colname1", "colname2"],
                          getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
            Handler of the underlying data.
        """

        # ------------------------------------------------------------

        if not isinstance(pandas_df, pd.DataFrame):
            raise TypeError("'pandas_df' must be of type pandas.DataFrame.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # ------------------------------------------------------------

        if metadata := pandas_df.attrs.get("getml"):
            sniffed_roles = Roles.from_dict(metadata["roles"])
        else:
            sniffed_roles = sniff_schema(
                pa.Schema.from_pandas(pandas_df, preserve_index=False)
            )

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_pandas(pandas_df=pandas_df, append=False)

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_parquet(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
        colnames: Iterable[str] = (),
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_parquet(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
        colnames: Iterable[str] = (),
    ) -> Roles: ...

    @classmethod
    def from_parquet(
        cls,
        fnames: Union[str, Iterable[str]],
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
        colnames: Iterable[str] = (),
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from parquet files.

        This is one of the fastest way to get data into the
        getML Engine.

        Args:
            fnames:
                The path of the parquet file(s) to be read.

            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
            Handler of the underlying data.
        """

        # ------------------------------------------------------------

        if isinstance(fnames, str):
            fnames = [fnames]

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # ------------------------------------------------------------

        sniffed_roles = sniff_parquet(fnames, colnames)

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_parquet(fnames=fnames, append=False, colnames=colnames)

    # --------------------------------------------------------------------

    @overload
    @classmethod
    def from_pyspark(
        cls,
        spark_df: pyspark.sql.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_pyspark(
        cls,
        spark_df: pyspark.sql.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_pyspark(
        cls,
        spark_df: pyspark.sql.DataFrame,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from a `pyspark.sql.DataFrame`.

        It will construct a data frame object in the Engine, fill it
        with the data read from the `pyspark.sql.DataFrame`, and
        return a corresponding [`DataFrame`][getml.DataFrame] handle.

        Args:
            spark_df:
                The table to be read.

            name:
                Name of the data frame to be created.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```

                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
                Handler of the underlying data.
        """

        # ------------------------------------------------------------

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # ------------------------------------------------------------

        head = spark_df.limit(2).toPandas()

        sniffed_roles = sniff_schema(pa.Schema.from_pandas(head))

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_pyspark(spark_df=spark_df, append=False)

    # ------------------------------------------------------------

    @overload
    @classmethod
    def from_query(
        cls,
        query: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
        conn: Optional[Connection] = None,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_query(
        cls,
        query: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
        conn: Optional[Connection] = None,
    ) -> Roles: ...

    @classmethod
    def from_query(
        cls,
        query: str,
        name: str,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
        conn: Optional[Connection] = None,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from a query run on a database.

        It will construct a data frame object in the engine, fill it
        with the data read from the query executed on the connected
        database (see [`database`][getml.database]), and return a
        corresponding [`DataFrame`][getml.DataFrame] handle.

        Args:
            query:
                The SQL query to be read.

            name:
                Name of the data frame to be created.

                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```

                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

            conn:
                The [`database.Connection`][getml.database.Connection] to be used.
                If you don't explicitly pass a connection, the engine
                will use the default connection.

        Returns:
                Handler of the underlying data.

        ??? example
            ```python
            getml.database.connect_mysql(
                host="relational.fel.cvut.cz"",
                port=3306,
                dbname="financial",
                user="guest",
                password="ctu-relational"
            )

            loan = getml.DataFrame.from_query(
                query='SELECT * FROM "loan";', name='loan')
            ```
        """

        # -------------------------------------------

        if not isinstance(query, str):
            raise TypeError("'query' must be str.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        # The content of roles is checked in the class constructor called below.
        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError(
                "'roles' must be a getml.data.Roles object, a dict or None."
            )

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # -------------------------------------------

        conn = conn or database.Connection()

        # ------------------------------------------------------------

        sniffed_roles = _sniff_query(query, name, conn)

        roles = _prepare_roles(roles, sniffed_roles, ignore_sniffed_roles=ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_query(query=query, append=False, conn=conn)

    # ------------------------------------------------------------

    @overload
    @classmethod
    def from_s3(
        cls,
        bucket: str,
        keys: Iterable[str],
        region: str,
        name: str,
        num_lines_sniffed: int = 1000,
        num_lines_read: int = 0,
        sep: str = ",",
        skip: int = 0,
        colnames: Optional[Iterable[str]] = None,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_s3(
        cls,
        bucket: str,
        keys: Iterable[str],
        region: str,
        name: str,
        num_lines_sniffed: int = 1000,
        num_lines_read: int = 0,
        sep: str = ",",
        skip: int = 0,
        colnames: Optional[Iterable[str]] = None,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_s3(
        cls,
        bucket: str,
        keys: Iterable[str],
        region: str,
        name: str,
        num_lines_sniffed: int = 1000,
        num_lines_read: int = 0,
        sep: str = ",",
        skip: int = 0,
        colnames: Optional[Iterable[str]] = None,
        roles: Optional[Union[Dict[Union[Role, str], Iterable[str]], Roles]] = None,
        ignore: bool = False,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from CSV files located in an S3 bucket.

        This classmethod will construct a data
        frame object in the Engine, fill it with the data read from
        the CSV file(s), and return a corresponding
        [`DataFrame`][getml.DataFrame] handle.

        enterprise-adm: Enterprise edition
            This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

            For licensing information and technical support, please [contact us][contact-page].

        Note:
            Note that S3 is not supported on Windows.

        Args:
            bucket:
                The bucket from which to read the files.

            keys:
                The list of keys (files in the bucket) to be read.

            region:
                The region in which the bucket is located.

            name:
                Name of the data frame to be created.

            num_lines_sniffed:
                Number of lines analyzed by the sniffer.

            num_lines_read:
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            sep:
                The separator used for separating fields.

            skip:
                Number of lines to skip at the beginning of each file.

            colnames:
                The first line of a CSV file
                usually contains the column names. When this is not the case,
                you need to explicitly pass them.

            roles:
                Maps the [`roles`][getml.data.roles] to the
                column names (see [`colnames`][getml.DataFrame.colnames]).

                The `roles` dictionary is expected to have the following format:
                ```python
                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}
                ```
                Otherwise, you can use the [`Roles`][getml.data.Roles] class.

            ignore:
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry:
                If set to True, the data will not be read. Instead, the method
                will return the inferred roles.

        Returns:
                Handler of the underlying data.

        ??? example
            Let's assume you have two CSV files - *file1.csv* and
            *file2.csv* - in the bucket. You can
            import their data into the getML Engine using the following
            commands:
            ```python
            getml.engine.set_s3_access_key_id("YOUR-ACCESS-KEY-ID")
            getml.engine.set_s3_secret_access_key("YOUR-SECRET-ACCESS-KEY")

            data_frame_expd = data.DataFrame.from_s3(
                bucket="your-bucket-name",
                keys=["file1.csv", "file2.csv"],
                region="us-east-2",
                name="MY DATA FRAME",
                sep=';'
            )
            ```

            You can also set the access credential as environment variables
            before you launch the getML Engine.

            Also refer to the documentation on [`from_csv`][getml.DataFrame.from_csv]
            for further information on overriding the CSV sniffer for greater
            type safety.

        """

        if isinstance(keys, str):
            keys = [keys]

        if not isinstance(bucket, str):
            raise TypeError("'bucket' must be str.")

        if not _is_non_empty_typed_list(keys, str):
            raise TypeError("'keys' must be either a string or a list of str")

        if not isinstance(region, str):
            raise TypeError("'region' must be str.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        if not isinstance(num_lines_sniffed, numbers.Real):
            raise TypeError("'num_lines_sniffed' must be a real number")

        if not isinstance(num_lines_read, numbers.Real):
            raise TypeError("'num_lines_read' must be a real number")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be str.")

        if not isinstance(skip, numbers.Real):
            raise TypeError("'skip' must be a real number")

        if roles is not None and not isinstance(roles, (dict, Roles)):
            raise TypeError("'roles' must be a geml.data.Roles object, a dict or None.")

        if not isinstance(ignore, bool):
            raise TypeError("'ignore' must be bool.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        if colnames is not None and not _is_non_empty_typed_list(colnames, str):
            raise TypeError(
                "'colnames' must be either be None or a non-empty list of str."
            )

        sniffed_roles = _sniff_s3(
            bucket=bucket,
            keys=keys,
            region=region,
            num_lines_sniffed=int(num_lines_sniffed),
            sep=sep,
            skip=int(skip),
            colnames=colnames,
        )

        roles = _prepare_roles(roles, sniffed_roles, ignore)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_s3(
            bucket=bucket,
            keys=keys,
            region=region,
            append=False,
            sep=sep,
            num_lines_read=int(num_lines_read),
            skip=int(skip),
            colnames=colnames,
        )

    # ------------------------------------------------------------

    @overload
    @classmethod
    def from_view(
        cls,
        view: View,
        name: str,
        dry: Literal[False] = False,
    ) -> DataFrame: ...

    @overload
    @classmethod
    def from_view(
        cls,
        view: View,
        name: str,
        dry: Literal[True] = True,
    ) -> Roles: ...

    @classmethod
    def from_view(
        cls,
        view: View,
        name: str,
        dry: bool = False,
    ) -> Union[DataFrame, Roles]:
        """Create a DataFrame from a [`View`][getml.data.View].

        This classmethod will construct a data
        frame object in the Engine, fill it with the data read from
        the [`View`][getml.data.View], and return a corresponding
        [`DataFrame`][getml.DataFrame] handle.

        Args:
            view:
                The view from which we want to read the data.

            name:
                Name of the data frame to be created.

            dry:
                If set to True, the data will not be read. Instead, the method
                will return an empty data frame with the roles set as inferred.

        Returns:
                Handler of the underlying data.


        """
        # ------------------------------------------------------------

        if not isinstance(view, View):
            raise TypeError("'view' must be getml.data.View.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        if not isinstance(dry, bool):
            raise TypeError("'dry' must be bool.")

        # ------------------------------------------------------------

        if dry:
            return view.roles

        data_frame = cls(name, view.roles)

        # ------------------------------------------------------------

        return data_frame.read_view(view=view, append=False)

    # ------------------------------------------------------------

    def iter_batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> Iterator[View]:
        yield from _iter_batches(self, batch_size)

    # ------------------------------------------------------------

    @property
    def _join_key_names(self) -> Iterable[str]:
        return [col.name for col in self._join_key_columns]

    # ------------------------------------------------------------

    @property
    def last_change(self) -> str:
        """
        A string describing the last time this data frame has been changed.
        """
        return _last_change(self.name)

    # ------------------------------------------------------------

    def load(self) -> DataFrame:
        """Loads saved data from disk.

        The data frame object holding the same name as the current
        [`DataFrame`][getml.DataFrame] instance will be loaded from
        disk into the getML Engine and updates the current handler
        using [`refresh`][getml.DataFrame.refresh].

        ??? example
            First, we have to create and import data sets.
            ```python
            d, _ = getml.datasets.make_numerical(population_name = 'test')
            getml.data.list_data_frames()
            ```

            In the output of [`list_data_frames`][getml.data.list_data_frames] we
            can find our underlying data frame object 'test' listed
            under the 'in_memory' key (it was created and imported by
            [`make_numerical`][getml.datasets.make_numerical]). This means the
            getML Engine does only hold it in memory (RAM) yet, and we
            still have to [`save`][getml.DataFrame.save] it to
            disk in order to [`load`][getml.DataFrame.load] it
            again or to prevent any loss of information between
            different sessions.
            ```python
            d.save()
            getml.data.list_data_frames()
            d2 = getml.DataFrame(name = 'test').load()
            ```

        Returns:
                Updated handle the underlying data frame in the getML
                Engine.

        Note:
            When invoking [`load`][getml.DataFrame.load] all
            changes of the underlying data frame object that took
            place after the last call to the
            [`save`][getml.DataFrame.save] method will be
            lost. Thus, this method  enables you to undo changes
            applied to the [`DataFrame`][getml.DataFrame].
            ```python
            d, _ = getml.datasets.make_numerical()
            d.save()

            # Accidental change we want to undo
            d.rm('column_01')

            d.load()
            ```
            If [`save`][getml.DataFrame.save] hasn't been called
            on the current instance yet, or it wasn't stored to disk in
            a previous session, [`load`][getml.DataFrame.load]
            will throw an exception

                File or directory '../projects/X/data/Y/' not found!

            Alternatively, [`load_data_frame`][getml.data.load_data_frame]
            offers an easier way of creating
            [`DataFrame`][getml.DataFrame] handlers to data in the
            getML Engine.

        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.load"
        cmd["name_"] = self.name
        comm.send(cmd)
        return self.refresh()

    # ------------------------------------------------------------

    @property
    def memory_usage(self):
        """
        Convenience wrapper that returns the memory usage in MB.
        """
        return self.nbytes() / 1e06

    # ------------------------------------------------------------

    @property
    def _monitor_url(self) -> Optional[str]:
        """
        A link to the data frame in the getML Monitor.
        """
        url = comm._monitor_url()
        return (
            url + "getdataframe/" + comm._get_project_name() + "/" + self.name + "/"
            if url
            else None
        )

    # ------------------------------------------------------------

    def nbytes(self) -> np.uint64:
        """Size of the data stored in the underlying data frame in the getML
        Engine.

        Returns:
                Size of the underlying object in bytes.

        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.nbytes"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                sock.close()
                comm.handle_engine_exception(msg)
            nbytes = comm.recv_string(sock)

        return np.uint64(nbytes)

    # ------------------------------------------------------------

    def ncols(self) -> int:
        """
        Number of columns in the current instance.

        Returns:
                Overall number of columns
        """
        return len(self.colnames)

    # ------------------------------------------------------------

    def nrows(self) -> int:
        """
        Number of rows in the current instance.

        Returns:
                Overall number of rows
        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.nrows"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                sock.close()
                comm.handle_engine_exception(msg)
            nrows = comm.recv_string(sock)

        return int(nrows)

    # ------------------------------------------------------------

    @property
    def _numerical_names(self):
        return [col.name for col in self._numerical_columns]

    # --------------------------------------------------------------------------

    def read_arrow(
        self,
        table: Union[pa.RecordBatch, pa.Table, Iterable[pa.RecordBatch]],
        append: bool = False,
    ) -> DataFrame:
        """Uploads a `pyarrow.Table` or `pyarrow.RecordBatch` to the getML Engine.

        Replaces the actual content of the underlying data frame in
        the getML Engine with `table`.

        Args:
            table:
                The arrow tablelike to be read as a `DataFrame`.

            append:
                If a data frame object holding the same `name` is
                already present in the getML Engine, should the content in
                `query` be appended or replace the existing data?

        Returns:
                Current instance.

        Note:
            For columns containing `pandas.Timestamp` there can
            be small inconsistencies in the order of microseconds
            when sending the data to the getML Engine. This is due to
            the way the underlying information is stored.
        """

        # ------------------------------------------------------------

        inferred_schema, batches = to_arrow_batches(table)

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        # ------------------------------------------------------------

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_pandas(...)."""
            )

        # ------------------------------------------------------------

        preprocessed_schema = preprocess_arrow_schema(inferred_schema, self.roles)
        batches = (cast_arrow_batch(batch, preprocessed_schema) for batch in batches)

        read_arrow_batches(batches, preprocessed_schema, self, append)

        return self.refresh()

    # --------------------------------------------------------------------------

    def read_csv(
        self,
        fnames: Iterable[str],
        append: bool = False,
        quotechar: str = '"',
        sep: str = ",",
        num_lines_read: int = 0,
        skip: int = 0,
        colnames: Optional[Iterable[str]] = None,
        time_formats: Optional[Iterable[str]] = None,
        verbose: bool = True,
        block_size: int = DEFAULT_CSV_READ_BLOCK_SIZE,
        in_batches: bool = False,
    ) -> DataFrame:
        """Read CSV files.

        It is assumed that the first line of each CSV file contains a
        header with the column names.

        Args:
            fnames:
                CSV file paths to be read.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                the CSV files in `fnames` be appended or replace the
                existing data?

            quotechar:
                The character used to wrap strings.

            sep:
                The separator used for separating fields.

            num_lines_read:
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            skip:
                Number of lines to skip at the beginning of each file.

            colnames:
                The first line of a CSV file
                usually contains the column names.
                When this is not the case, you need to explicitly pass them.

            time_formats:
                The list of formats tried when parsing time stamps.

                The formats are allowed to contain the following
                special characters:

                * %w - abbreviated weekday (Mon, Tue, ...)
                * %W - full weekday (Monday, Tuesday, ...)
                * %b - abbreviated month (Jan, Feb, ...)
                * %B - full month (January, February, ...)
                * %d - zero-padded day of month (01 .. 31)
                * %e - day of month (1 .. 31)
                * %f - space-padded day of month ( 1 .. 31)
                * %m - zero-padded month (01 .. 12)
                * %n - month (1 .. 12)
                * %o - space-padded month ( 1 .. 12)
                * %y - year without century (70)
                * %Y - year with century (1970)
                * %H - hour (00 .. 23)
                * %h - hour (00 .. 12)
                * %a - am/pm
                * %A - AM/PM
                * %M - minute (00 .. 59)
                * %S - second (00 .. 59)
                * %s - seconds and microseconds (equivalent to %S.%F)
                * %i - millisecond (000 .. 999)
                * %c - centisecond (0 .. 9)
                * %F - fractional seconds/microseconds (000000 - 999999)
                * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
                * %Z - time zone differential in RFC format (GMT or +NNNN)
                * %% - percent sign

            verbose:
                If True, when `fnames` are urls, the filenames are printed to
                stdout during the download.

            block_size:
                The number of bytes read with each batch. Passed down to
                pyarrow.

            in_batches:
                If True, read blocks streamwise manner and send those batches to
                the engine. Blocks are read and sent to the engine sequentially.
                While more memory efficient, streaming in batches is slower as
                it is inherently single-threaded. If False (default) the data is
                read with multiple threads into arrow first and sent to the engine
                afterwards.

        Returns:
                Handler of the underlying data.

        """

        time_formats = time_formats or constants.TIME_FORMATS

        if isinstance(fnames, str):
            fnames = [fnames]

        if not _is_non_empty_typed_list(fnames, str):
            raise TypeError("'fnames' must be either a string or a list of str")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if not isinstance(quotechar, str):
            raise TypeError("'quotechar' must be str.")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be str.")

        if not isinstance(num_lines_read, numbers.Real):
            raise TypeError("'num_lines_read' must be a real number")

        if not isinstance(skip, numbers.Real):
            raise TypeError("'skip' must be a real number")

        if not _is_non_empty_typed_list(time_formats, str):
            raise TypeError("'time_formats' must be a non-empty list of str")

        if colnames:
            if not _is_iterable_not_str_of_type(colnames, str):
                raise TypeError("'colnames' must be an iterable of str")

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_csv(...)."""
            )

        if not _is_non_empty_typed_list(fnames, str):
            raise TypeError(
                """'fnames' must be a list containing at
                least one path to a CSV file"""
            )

        fnames_ = _retrieve_urls(fnames, verbose)

        if colnames is None:
            colnames = ()

        stream_read_csv = stream_csv if in_batches else read_csv

        readers = (
            stream_read_csv(
                Path(fname),
                roles=self.roles,
                skip_rows=skip,
                column_names=colnames,
                delimiter=sep,
                quote_char=quotechar,
                block_size=block_size,
            )
            for fname in fnames_
        )

        for batches in readers:
            first_batch = next(batches)
            schema = first_batch.schema
            read_arrow_batches(iter((first_batch, *batches)), schema, self, append)
            if not append:
                append = True

        return self

    # --------------------------------------------------------------------------

    def read_json(
        self,
        json_str: str,
        append: bool = False,
        time_formats: Optional[Iterable[str]] = None,
    ) -> DataFrame:
        """Fill from JSON

        Fills the data frame with data from a JSON string.

        Args:

            json_str:
                The JSON string containing the data.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                `json_str` be appended or replace the existing data?

            time_formats:
                The list of formats tried when parsing time stamps.
                The formats are allowed to contain the following
                special characters:

                * %w - abbreviated weekday (Mon, Tue, ...)
                * %W - full weekday (Monday, Tuesday, ...)
                * %b - abbreviated month (Jan, Feb, ...)
                * %B - full month (January, February, ...)
                * %d - zero-padded day of month (01 .. 31)
                * %e - day of month (1 .. 31)
                * %f - space-padded day of month ( 1 .. 31)
                * %m - zero-padded month (01 .. 12)
                * %n - month (1 .. 12)
                * %o - space-padded month ( 1 .. 12)
                * %y - year without century (70)
                * %Y - year with century (1970)
                * %H - hour (00 .. 23)
                * %h - hour (00 .. 12)
                * %a - am/pm
                * %A - AM/PM
                * %M - minute (00 .. 59)
                * %S - second (00 .. 59)
                * %s - seconds and microseconds (equivalent to %S.%F)
                * %i - millisecond (000 .. 999)
                * %c - centisecond (0 .. 9)
                * %F - fractional seconds/microseconds (000000 - 999999)
                * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
                * %Z - time zone differential in RFC format (GMT or +NNNN)
                * %% - percent sign

        Returns:
                Handler of the underlying data.

        Note:
            This does not support NaN values. If you want support for NaN,
            use [`from_json`][getml.DataFrame.from_json] instead.

        """

        time_formats = time_formats or constants.TIME_FORMATS

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_json(...)."""
            )

        if not isinstance(json_str, str):
            raise TypeError("'json_str' must be of type str")

        if not isinstance(append, bool):
            raise TypeError("'append' must be of type bool")

        if not _is_non_empty_typed_list(time_formats, str):
            raise TypeError(
                """'time_formats' must be a list of strings
                containing at least one time format"""
            )

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.from_json"
        cmd["name_"] = self.name

        cmd["categorical_"] = self._categorical_names
        cmd["join_keys_"] = self._join_key_names
        cmd["numerical_"] = self._numerical_names
        cmd["targets_"] = self._target_names
        cmd["text_"] = self._text_names
        cmd["time_stamps_"] = self._time_stamp_names
        cmd["unused_floats_"] = self._unused_float_names
        cmd["unused_strings_"] = self._unused_string_names

        cmd["append_"] = append
        cmd["time_formats_"] = time_formats

        with comm.send_and_get_socket(cmd) as sock:
            comm.send_string(sock, json_str)
            msg = comm.recv_string(sock)

        if msg != "Success!":
            comm.handle_engine_exception(msg)

        return self

    # --------------------------------------------------------------------------

    def read_parquet(
        self,
        fnames: Union[str, Iterable[str]],
        append: bool = False,
        verbose: bool = False,
        colnames: Iterable[str] = (),
    ) -> DataFrame:
        """Read a parquet file.

        Args:
            fnames:
                The filepath of the parquet file(s) to be read.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                the CSV files in `fnames` be appended or replace the
                existing data?

            verbose:
                If True, when `fnames` are urls, the filenames are printed to
                stdout during the download.

        Returns:
            Handler of the underlying data.
        """

        if isinstance(fnames, str):
            fnames = [fnames]

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if not colnames:
            colnames = self.colnames

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than
                zero columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_parquet(...)."""
            )

        fnames = _retrieve_urls(fnames, verbose)

        readers = (pq.ParquetFile(fname) for fname in fnames)

        for reader in readers:
            inferred_schema = reader.schema_arrow
            preprocessed_schema = preprocess_arrow_schema(inferred_schema, self.roles)
            cast_batches = (
                cast_arrow_batch(batch, preprocessed_schema)
                for batch in reader.iter_batches(columns=colnames)
            )
            read_arrow_batches(
                cast_batches,
                preprocessed_schema,
                self,
                append,
            )
            if not append:
                append = True

        return self

    # --------------------------------------------------------------------------

    def read_s3(
        self,
        bucket: str,
        keys: Iterable[str],
        region: str,
        append: bool = False,
        sep: str = ",",
        num_lines_read: int = 0,
        skip: int = 0,
        colnames: Optional[Iterable[str]] = None,
        time_formats: Optional[Iterable[str]] = None,
    ) -> DataFrame:
        """Read CSV files from an S3 bucket.

        It is assumed that the first line of each CSV file contains a
        header with the column names.

        enterprise-adm: Enterprise edition
            This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

            For licensing information and technical support, please [contact us][contact-page].

        Note:
            Note that S3 is not supported on Windows.

        Args:
            bucket:
                The bucket from which to read the files.

            keys:
                The list of keys (files in the bucket) to be read.

            region:
                The region in which the bucket is located.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                the CSV files in `fnames` be appended or replace the
                existing data?

            sep:
                The separator used for separating fields.

            num_lines_read:
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            skip:
                Number of lines to skip at the beginning of each file.

            colnames:
                The first line of a CSV file
                usually contains the column names.
                When this is not the case, you need to explicitly pass them.

            time_formats:
                The list of formats tried when parsing time stamps.

                The formats are allowed to contain the following
                special characters:

                * %w - abbreviated weekday (Mon, Tue, ...)
                * %W - full weekday (Monday, Tuesday, ...)
                * %b - abbreviated month (Jan, Feb, ...)
                * %B - full month (January, February, ...)
                * %d - zero-padded day of month (01 .. 31)
                * %e - day of month (1 .. 31)
                * %f - space-padded day of month ( 1 .. 31)
                * %m - zero-padded month (01 .. 12)
                * %n - month (1 .. 12)
                * %o - space-padded month ( 1 .. 12)
                * %y - year without century (70)
                * %Y - year with century (1970)
                * %H - hour (00 .. 23)
                * %h - hour (00 .. 12)
                * %a - am/pm
                * %A - AM/PM
                * %M - minute (00 .. 59)
                * %S - second (00 .. 59)
                * %s - seconds and microseconds (equivalent to %S.%F)
                * %i - millisecond (000 .. 999)
                * %c - centisecond (0 .. 9)
                * %F - fractional seconds/microseconds (000000 - 999999)
                * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
                * %Z - time zone differential in RFC format (GMT or +NNNN)
                * %% - percent sign

        Returns:
                Handler of the underlying data.

        """

        time_formats = time_formats or constants.TIME_FORMATS

        if isinstance(keys, str):
            keys = [keys]

        if not isinstance(bucket, str):
            raise TypeError("'bucket' must be str.")

        if not _is_non_empty_typed_list(keys, str):
            raise TypeError("'keys' must be either a string or a list of str")

        if not isinstance(region, str):
            raise TypeError("'region' must be str.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be str.")

        if not isinstance(num_lines_read, numbers.Real):
            raise TypeError("'num_lines_read' must be a real number")

        if not isinstance(skip, numbers.Real):
            raise TypeError("'skip' must be a real number")

        if not _is_non_empty_typed_list(time_formats, str):
            raise TypeError("'time_formats' must be a non-empty list of str")

        if colnames is not None and not _is_non_empty_typed_list(colnames, str):
            raise TypeError(
                "'colnames' must be either be None or a non-empty list of str."
            )

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_s3(...)."""
            )

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.read_s3"
        cmd["name_"] = self.name

        cmd["append_"] = append
        cmd["bucket_"] = bucket
        cmd["keys_"] = keys
        cmd["region_"] = region
        cmd["sep_"] = sep
        cmd["time_formats_"] = time_formats
        cmd["num_lines_read_"] = num_lines_read
        cmd["skip_"] = skip

        if colnames is not None:
            cmd["colnames_"] = colnames

        cmd["categorical_"] = self._categorical_names
        cmd["join_keys_"] = self._join_key_names
        cmd["numerical_"] = self._numerical_names
        cmd["targets_"] = self._target_names
        cmd["text_"] = self._text_names
        cmd["time_stamps_"] = self._time_stamp_names
        cmd["unused_floats_"] = self._unused_float_names
        cmd["unused_strings_"] = self._unused_string_names

        comm.send(cmd)

        return self

    # ------------------------------------------------------------

    def read_view(
        self,
        view: View,
        append: bool = False,
    ) -> DataFrame:
        """Read the data from a [`View`][getml.data.View].

        Args:
            view:
                The view to read.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                the CSV files in `fnames` be appended or replace the
                existing data?

        Returns:
                Handler of the underlying data.

        """

        if not isinstance(view, View):
            raise TypeError("'view' must be getml.data.View.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        view.check()

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.from_view"
        cmd["name_"] = self.name

        cmd["view_"] = view._getml_deserialize()

        cmd["append_"] = append

        comm.send(cmd)

        return self.refresh()

    # --------------------------------------------------------------------------

    def read_db(
        self, table_name: str, append: bool = False, conn: Optional[Connection] = None
    ) -> DataFrame:
        """
        Fill from Database.

        The DataFrame will be filled from a table in the database.

        Args:
            table_name:
                Table from which we want to retrieve the data.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                `table_name` be appended or replace the existing data?

            conn:
                The database connection to be used.
                If you don't explicitly pass a connection,
                the Engine will use the default connection.

        Returns:
                Handler of the underlying data.
        """

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be str.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_db(...)."""
            )

        conn = conn or database.Connection()

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.from_db"
        cmd["name_"] = self.name
        cmd["table_name_"] = table_name

        cmd["categorical_"] = self._categorical_names
        cmd["join_keys_"] = self._join_key_names
        cmd["numerical_"] = self._numerical_names
        cmd["targets_"] = self._target_names
        cmd["text_"] = self._text_names
        cmd["time_stamps_"] = self._time_stamp_names
        cmd["unused_floats_"] = self._unused_float_names
        cmd["unused_strings_"] = self._unused_string_names

        cmd["append_"] = append

        cmd["conn_id_"] = conn.conn_id

        comm.send(cmd)

        return self

    # --------------------------------------------------------------------------

    def read_pandas(self, pandas_df: pd.DataFrame, append: bool = False) -> DataFrame:
        """Uploads a `pandas.DataFrame`.

        Replaces the actual content of the underlying data frame in
        the getML Engine with `pandas_df`.

        Args:
            pandas_df:
                Data the underlying data frame object in the getML
                Engine should obtain.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML Engine, should the content in
                `query` be appended or replace the existing data?

        Returns:
                Handler of the underlying data.
        Note:
            For columns containing `pandas.Timestamp` there can
            occur small inconsistencies in the order of microseconds
            when sending the data to the getML Engine. This is due to
            the way the underlying information is stored.
        """

        if not isinstance(pandas_df, pd.DataFrame):
            raise TypeError("'pandas_df' must be of type pandas.DataFrame.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_pandas(...)."""
            )

        table = pa.Table.from_pandas(pandas_df[self.columns])

        return self.read_arrow(table, append=append)

    # --------------------------------------------------------------------------

    def read_pyspark(
        self, spark_df: pyspark.sql.DataFrame, append: bool = False
    ) -> DataFrame:
        """Uploads a `pyspark.sql.DataFrame`.

        Replaces the actual content of the underlying data frame in
        the getML Engine with `pandas_df`.

        Args:
            spark_df:
                Data the underlying data frame object in the getML
                Engine should obtain.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML Engine, should the content in
                `query` be appended or replace the existing data?

        Returns:
                Handler of the underlying data.
        """

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        temp_dir = _retrieve_temp_dir()
        path = temp_dir / str(self.name)
        spark_df.write.mode("overwrite").parquet(str(path))

        filepaths = [
            os.path.join(path, filepath)
            for filepath in os.listdir(path)
            if filepath[-8:] == ".parquet"
        ]

        for i, filepath in enumerate(filepaths):
            self.read_parquet(filepath, append or i > 0)

        shutil.rmtree(path)

        return self

    # --------------------------------------------------------------------------

    def read_query(
        self,
        query: str,
        append: Optional[bool] = False,
        conn: Optional[Connection] = None,
    ) -> DataFrame:
        """Fill from query

        Fills the data frame with data from a table in the database.

        Args:
            query:
                The query used to retrieve the data.

            append:
                If a data frame object holding the same ``name`` is
                already present in the getML Engine, should the content in
                `query` be appended or replace the existing data?

            conn:
                The database connection to be used.
                If you don't explicitly pass a connection,
                the Engine will use the default connection.

        Returns:
                Handler of the underlying data.
        """

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than zero
                columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_db(...)."""
            )

        if not isinstance(query, str):
            raise TypeError("'query' must be of type str")

        if not isinstance(append, bool):
            raise TypeError("'append' must be of type bool")

        conn = conn or database.Connection()

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.from_query"
        cmd["name_"] = self.name
        cmd["query_"] = query

        cmd["categorical_"] = self._categorical_names
        cmd["join_keys_"] = self._join_key_names
        cmd["numerical_"] = self._numerical_names
        cmd["targets_"] = self._target_names
        cmd["text_"] = self._text_names
        cmd["time_stamps_"] = self._time_stamp_names
        cmd["unused_floats_"] = self._unused_float_names
        cmd["unused_strings_"] = self._unused_string_names

        cmd["append_"] = append

        cmd["conn_id_"] = conn.conn_id

        comm.send(cmd)

        return self

    # --------------------------------------------------------------------------

    def refresh(self) -> DataFrame:
        """Aligns meta-information of the current instance with the
                corresponding data frame in the getML Engine.

                This method can be used to avoid encoding conflicts. Note that
                [`load`][getml.DataFrame.load] as well as several other
                methods automatically call [`refresh`][getml.DataFrame.refresh].

        Returns:
                Updated handle the underlying data frame in the getML
                Engine.

        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.refresh"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)

        if msg[0] != "{":
            comm.handle_engine_exception(msg)

        roles = json.loads(msg)

        self.__init__(name=cast(str, self.name), roles=roles)

        return self

    # ------------------------------------------------------------

    @property
    def roles(self):
        """
        The roles of the columns included
        in this DataFrame.
        """
        return Roles(
            categorical=tuple(self._categorical_names),
            join_key=tuple(self._join_key_names),
            numerical=tuple(self._numerical_names),
            target=tuple(self._target_names),
            text=tuple(self._text_names),
            time_stamp=tuple(self._time_stamp_names),
            unused_float=tuple(self._unused_float_names),
            unused_string=tuple(self._unused_string_names),
        )

    # ------------------------------------------------------------

    def remove_subroles(
        self,
        cols: Union[
            str, FloatColumn, StringColumn, List[Union[str, FloatColumn, StringColumn]]
        ],
    ) -> None:
        """Removes all [`subroles`][getml.data.subroles] from one or more columns.

        Args:
            cols:
                The columns or the names thereof.
        """

        names = _handle_cols(cols)

        for name in names:
            self._set_subroles(name, append=False, subroles=[])

        self.refresh()

    # ------------------------------------------------------------

    def remove_unit(
        self,
        cols: Union[
            str, FloatColumn, StringColumn, List[Union[str, FloatColumn, StringColumn]]
        ],
    ):
        """Removes the unit from one or more columns.

        Args:
            cols:
                The columns or the names thereof.
        """

        names = _handle_cols(cols)

        for name in names:
            self._set_unit(name, "")

        self.refresh()

    # ------------------------------------------------------------

    @property
    def rowid(self):
        """
        The rowids for this data frame.
        """
        return rowid()[: self.nrows()]

    # ------------------------------------------------------------

    def save(self) -> DataFrame:
        """Writes the underlying data in the getML Engine to disk.

        Returns:
                The current instance.

        """

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.save"
        cmd["name_"] = self.name

        comm.send(cmd)

        return self

    # ------------------------------------------------------------

    def set_role(
        self,
        cols: Union[
            str, FloatColumn, StringColumn, List[Union[str, FloatColumn, StringColumn]]
        ],
        role: str,
        time_formats: Optional[Iterable[str]] = None,
    ):
        """Assigns a new role to one or more columns.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The `time_formats` argument is used to interpret [Time Stamps][annotating-data-time-stamp]. For more information on
        roles, please refer to the [User Guide][annotating-data].

        Args:
            cols:
                The columns or the names of the columns.

            role:
                The role to be assigned.

            time_formats:
                Formats to be used to parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.

        ??? example
            ```python
            data_df = dict(
                animal=["hawk", "parrot", "goose"],
                votes=[12341, 5127, 65311],
                date=["04/06/2019", "01/03/2019", "24/12/2018"])
            df = getml.DataFrame.from_dict(data_df, "animal_elections")
            df.set_role(['animal'], getml.data.roles.categorical)
            df.set_role(['votes'], getml.data.roles.numerical)
            df.set_role(
                ['date'], getml.data.roles.time_stamp, time_formats=['%d/%m/%Y'])

            df
            ```
            ```
            | date                        | animal      | votes     |
            | time stamp                  | categorical | numerical |
            ---------------------------------------------------------
            | 2019-06-04T00:00:00.000000Z | hawk        | 12341     |
            | 2019-03-01T00:00:00.000000Z | parrot      | 5127      |
            | 2018-12-24T00:00:00.000000Z | goose       | 65311     |
            ```
        """
        # ------------------------------------------------------------

        time_formats = time_formats or constants.TIME_FORMATS

        # ------------------------------------------------------------

        names = _handle_cols(cols)

        if not isinstance(role, str):
            raise TypeError("'role' must be str.")

        if not _is_non_empty_typed_list(time_formats, str):
            raise TypeError("'time_formats' must be a non-empty list of str")

        # ------------------------------------------------------------

        for nname in names:
            if nname not in self.colnames:
                raise ValueError("No column called '" + nname + "' found.")

        if role not in self._all_roles:
            raise ValueError(
                "'role' must be one of the following values: " + str(self._all_roles)
            )

        # ------------------------------------------------------------

        for name in names:
            if self[name].role != role:
                self._set_role(name, role, time_formats)

        # ------------------------------------------------------------

        self.refresh()

    # ------------------------------------------------------------

    def set_subroles(
        self,
        cols: Union[
            str, FloatColumn, StringColumn, List[Union[str, FloatColumn, StringColumn]]
        ],
        subroles: Optional[Union[Subrole, Iterable[str]]] = None,
        append: Optional[bool] = True,
    ):
        """Assigns one or several new [`subroles`][getml.data.subroles] to one or more columns.

        Args:
            cols:
                The columns or the names thereof.

            subroles:
                The subroles to be assigned.
                Must be from [`subroles`][getml.data.subroles].

            append:
                Whether you want to append the
                new subroles to the existing subroles.
        """

        names = _handle_cols(cols)

        if isinstance(subroles, str):
            subroles = [subroles]

        if not _is_non_empty_typed_list(subroles, str):
            raise TypeError("'subroles' must be either a string or a list of strings.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be a bool.")

        for name in names:
            self._set_subroles(name, append, subroles)

        self.refresh()

    # ------------------------------------------------------------

    def set_unit(
        self,
        cols: Union[
            str, FloatColumn, StringColumn, List[Union[str, FloatColumn, StringColumn]]
        ],
        unit: str,
        comparison_only: bool = False,
    ):
        """Assigns a new unit to one or more columns.

        Args:
            cols:
                The columns or the names thereof.

            unit:
                The unit to be assigned.

            comparison_only:
                Whether you want the column to
                be used for comparison only. This means that the column can
                only be used in comparison to other columns of the same unit.

                An example might be a bank account number: The number in itself
                is hardly interesting, but it might be useful to know how often
                we have seen that same bank account number in another table.
        """

        names = _handle_cols(cols)

        if not isinstance(unit, str):
            raise TypeError("Parameter 'unit' must be a str.")

        if comparison_only:
            unit += COMPARISON_ONLY

        for name in names:
            self._set_unit(name, unit)

        self.refresh()

    # ------------------------------------------------------------

    @property
    def shape(self):
        """
        A tuple containing the number of rows and columns of
        the DataFrame.
        """
        self.refresh()
        return (self.nrows(), self.ncols())

    # ------------------------------------------------------------

    @property
    def _target_names(self):
        return [col.name for col in self._target_columns]

    # ------------------------------------------------------------

    @property
    def _text_names(self):
        return [col.name for col in self._text_columns]

    # ------------------------------------------------------------

    @property
    def _time_stamp_names(self):
        return [col.name for col in self._time_stamp_columns]

    # ----------------------------------------------------------------

    def to_arrow(self) -> pa.Table:
        """Creates a `pyarrow.Table` from the current instance.

        Loads the underlying data from the getML Engine and constructs
        a `pyarrow.Table`.

        Returns:
                Pyarrow equivalent of the current instance including its underlying data.
        """
        return to_arrow(self)

    # ------------------------------------------------------------

    @contextmanager
    def to_arrow_stream(self) -> Iterator[pa.RecordBatchReader]:
        """
        Streams the dataframe as an Apache Arrow `pa.RecordBatchReader`.

        This method provides a way to access the dataframe as an Apache Arrow
        stream. Apache Arrow is a cross-language development platform for
        in-memory data that specifies a standardized language-independent
        columnar memory format. Using `to_arrow_stream` allows for efficient,
        zero-copy (or near zero-copy) data exchange with other systems that
        support Arrow, such as DuckDB, Pandas, Polars, and various data
        processing engines.

        The method is a context manager (used with a `with` statement). This
        ensures that any underlying resources associated with the stream,
        such as network connections or temporary files, are properly initialized
        when entering the `with` block and cleaned up when exiting.

        The `pa.RecordBatchReader` yielded by this context manager allows you to
        read the dataset iteratively as a sequence of Arrow `RecordBatch` objects.
        Each `RecordBatch` represents a chunk of the dataset's columns.

        Yields:
            pa.RecordBatchReader: An iterator-like object that yields Apache
            Arrow `RecordBatch` instances.

        Example:
            Integrating with DuckDB for SQL-based analysis:

            >>> import getml
            >>> import duckdb

            >>> getml.set_project("arrow_stream")

            >>> generated, _ = getml.datasets.make_numerical()

            >>> con = duckdb.connect()

            >>> # Use the context manager to get the Arrow stream
            >>> with generated.to_arrow_stream() as arrow_stream_reader:
            ...     # Register the Arrow stream as a duckdb relation
            ...     con.register("generated", arrow_stream_reader)
            ...
            ...     # Now you can query the data using SQL
            ...     count = con.execute("SELECT COUNT(*) FROM generated").df()
            ...     print(count)
            # count_star()
            # 500

            >>> with generated.to_arrow_stream() as arrow_stream_reader:
            ...     summary = con.execute("SUMMARIZE generated").df()
            ...     print(summary)
            #   column_name   column_type                            min                            max  ...                         q50                         q75 count null_percentage
            # 0    join_key       varchar                              0                             99  ...                        none                        none   500             0.0
            # 1   column_01        double            -0.9939631176936072             0.9966827171572035  ...       -0.012749915265777218         0.49808863342423293   500             0.0
            # 2     targets        double                            0.0                          152.0  ...          111.92857142857143          126.16666666666667   500             0.0
            # 3  time_stamp  timestamp_ns  1970-01-01 00:00:00.003686084  1970-01-01 00:00:00.998338199  ...  1970-01-01 00:00:00.497298  1970-01-01 00:00:00.747046   500             0.0

            # [4 rows x 12 columns]
        """
        with to_arrow_stream(self) as stream:
            yield stream

    # ------------------------------------------------------------

    def to_csv(
        self,
        fname: str,
        quotechar: str = '"',
        sep: str = ",",
        batch_size: int = DEFAULT_BATCH_SIZE,
        quoting_style: str = "needed",
    ):
        """
        Writes the underlying data into a newly created CSV file.

        Args:
            fname:
                The name of the CSV file.
                The ending ".csv" and an optional batch number will
                be added automatically.

            quotechar:
                The character used to wrap strings.

            sep:
                The character used for separating fields.

            batch_size:
                Maximum number of lines per file. Set to 0 to read
                the entire data frame into a single file.

            quoting_style (str):
                The quoting style to use. Delegated to pyarrow.

                The following values are accepted:
                - `"needed"` (default): only enclose values in quotes when needed.
                - `"all_valid"`: enclose all valid values in quotes; nulls are not
                  quoted.
                - `"none"`: do not enclose any values in quotes; values containing
                  special characters (such as quotes, cell delimiters or line
                  endings) will raise an error.

        Deprecated:
           1.5: The `quotechar` parameter is deprecated.
        """

        if quotechar != '"':
            warnings.warn(
                "'quotechar' is deprecated, use 'quoting_style' instead.",
                DeprecationWarning,
            )

        to_csv(self, fname, sep, batch_size, quoting_style)

    # ------------------------------------------------------------

    def to_db(self, table_name: str, conn: Optional[Connection] = None):
        """Writes the underlying data into a newly created table in the
        database.

        Args:
            table_name:
                Name of the table to be created.

                If a table of that name already exists, it will be
                replaced.

            conn:
                The database connection to be used.
                If you don't explicitly pass a connection,
                the Engine will use the default connection.
        """

        conn = conn or database.Connection()

        self.refresh()

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be of type str")

        cmd = {}

        cmd["type_"] = "DataFrame.to_db"
        cmd["name_"] = self.name

        cmd["table_name_"] = table_name

        cmd["conn_id_"] = conn.conn_id

        comm.send(cmd)

    # ----------------------------------------------------------------

    def to_html(self, max_rows: int = 10):
        """
        Represents the data frame in HTML format, optimized for an
        iPython notebook.

        Args:
            max_rows:
                The maximum number of rows to be displayed.
        """

        if not _exists_in_memory(self.name):
            return _empty_data_frame().replace("\n", "<br>")

        formatted = self._format()
        formatted.max_rows = max_rows

        footer = self._collect_footer_data()

        return formatted._render_html(footer=footer)

    # ------------------------------------------------------------

    def to_json(self):
        """Creates a JSON string from the current instance.

        Loads the underlying data from the getML Engine and constructs
        a JSON string.
        """
        return self.to_pandas().to_json()

    # ----------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """Creates a `pandas.DataFrame` from the current instance.

        Loads the underlying data from the getML Engine and constructs
        `pandas.DataFrame`.

        Returns:
                Pandas equivalent of the current instance including
                its underlying data.

        """
        table = to_arrow(self)
        df = table.to_pandas()
        df.attrs = {"getml": json.loads(table.schema.metadata[b"getml"])}
        return df

    # ------------------------------------------------------------

    def to_parquet(
        self,
        fname: str,
        compression: Literal["brotli", "gzip", "lz4", "snappy", "zstd"] = "snappy",
        coerce_timestamps: Optional[bool] = None,
    ) -> None:
        """
        Writes the underlying data into a newly created parquet file.

        Args:
            fname:
                The name of the parquet file.

            compression:
                The compression format to use.
                Supported values are "brotli", "gzip", "lz4", "snappy", "zstd"
            coerce_timestamps:
                Cast time stamps to a particular resolution.
                For details, refer to `pyarrow.parquet.ParquetWriter` docs.
        """
        to_parquet(self, fname, compression, coerce_timestamps=coerce_timestamps)

    # ----------------------------------------------------------------

    def to_placeholder(self, name: Optional[str] = None) -> Placeholder:
        """Generates a [`Placeholder`][getml.data.Placeholder] from the
        current [`DataFrame`][getml.DataFrame].

        Args:
            name:
                The name of the placeholder. If no
                name is passed, then the name of the placeholder will
                be identical to the name of the current data frame.

        Returns:
                A placeholder with the same name as this data frame.


        """
        self.refresh()
        return Placeholder(name=name or self.name, roles=self.roles)

    # ----------------------------------------------------------------
    def to_pyspark(
        self, spark: pyspark.sql.SparkSession, name: Optional[str] = None
    ) -> pyspark.sql.DataFrame:
        """Creates a `pyspark.sql.DataFrame` from the current instance.

        Loads the underlying data from the getML Engine and constructs
        a `pyspark.sql.DataFrame`.

        Args:
            spark:
                The pyspark session in which you want to
                create the data frame.

            name:
                The name of the temporary view to be created on top
                of the `pyspark.sql.DataFrame`,
                with which it can be referred to
                in Spark SQL (refer to
                `pyspark.sql.DataFrame.createOrReplaceTempView`).
                If None is passed, then the name of this
                [`DataFrame`][getml.DataFrame] will be used.

        Returns:
                Pyspark equivalent of the current instance including its underlying data.

        """
        return _to_pyspark(self, name, spark)

    # ------------------------------------------------------------

    def to_s3(
        self,
        bucket: str,
        key: str,
        region: str,
        sep: Optional[str] = ",",
        batch_size: Optional[int] = 50000,
    ):
        """
        Writes the underlying data into a newly created CSV file
        located in an S3 bucket.

        enterprise-adm: Enterprise edition
            This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

            For licensing information and technical support, please [contact us][contact-page].

        Note:
            Note that S3 is not supported on Windows.

        Args:
            bucket:
                The bucket from which to read the files.

            key:
                The key in the S3 bucket in which you want to
                write the output. The ending ".csv" and an optional
                batch number will be added automatically.

            region:
                The region in which the bucket is located.

            sep:
                The character used for separating fields.

            batch_size:
                Maximum number of lines per file. Set to 0 to read
                the entire data frame into a single file.

        ??? example
            ```python
            getml.engine.set_s3_access_key_id("YOUR-ACCESS-KEY-ID")
            getml.engine.set_s3_secret_access_key("YOUR-SECRET-ACCESS-KEY")

            your_df.to_s3(
                bucket="your-bucket-name",
                key="filename-on-s3",
                region="us-east-2",
                sep=';'
            )
            ```

        """

        self.refresh()

        if not isinstance(bucket, str):
            raise TypeError("'bucket' must be of type str")

        if not isinstance(key, str):
            raise TypeError("'fname' must be of type str")

        if not isinstance(region, str):
            raise TypeError("'region' must be of type str")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be of type str")

        if not isinstance(batch_size, numbers.Real):
            raise TypeError("'batch_size' must be a real number")

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.to_s3"
        cmd["name_"] = self.name

        cmd["bucket_"] = bucket
        cmd["key_"] = key
        cmd["region_"] = region
        cmd["sep_"] = sep
        cmd["batch_size_"] = batch_size

        comm.send(cmd)

    # ------------------------------------------------------------

    @property
    def _unused_float_names(self):
        return [col.name for col in self._unused_float_columns]

    # ------------------------------------------------------------

    @property
    def _unused_names(self):
        return self._unused_float_names + self._unused_string_names

    # ------------------------------------------------------------

    @property
    def _unused_string_names(self):
        return [col.name for col in self._unused_string_columns]

    # ------------------------------------------------------------

    def unload(self):
        """
        Unloads the data frame from memory.
        """

        # ------------------------------------------------------------

        self._delete(mem_only=True)

    # ------------------------------------------------------------

    def where(
        self,
        index: Union[
            numbers.Integral, slice, BooleanColumnView, FloatColumnView, FloatColumn
        ],
    ) -> View:
        """Extract a subset of rows.

        Creates a new [`View`][getml.data.View] as a
        subselection of the current instance.

        Args:
            index:
                Indicates the rows you want to select.

        Returns:
                A new [`View`][getml.data.View] containing the selected rows.

        ??? example
            Generate example data:
            ```python
            data = dict(
                fruit=["banana", "apple", "cherry", "cherry", "melon", "pineapple"],
                price=[2.4, 3.0, 1.2, 1.4, 3.4, 3.4],
                join_key=["0", "1", "2", "2", "3", "3"])

            fruits = getml.DataFrame.from_dict(data, name="fruits",
            roles={"categorical": ["fruit"], "join_key": ["join_key"], "numerical": ["price"]})

            fruits
            ```
            ```
            | join_key | fruit       | price     |
            | join key | categorical | numerical |
            --------------------------------------
            | 0        | banana      | 2.4       |
            | 1        | apple       | 3         |
            | 2        | cherry      | 1.2       |
            | 2        | cherry      | 1.4       |
            | 3        | melon       | 3.4       |
            | 3        | pineapple   | 3.4       |
            ```
            Apply where condition. This creates a new DataFrame called "cherries":

            ```python
            cherries = fruits.where(
                fruits["fruit"] == "cherry")

            cherries
            ```
            ```
            | join_key | fruit       | price     |
            | join key | categorical | numerical |
            --------------------------------------
            | 2        | cherry      | 1.2       |
            | 2        | cherry      | 1.4       |
            ```

        """

        return _where(self, index)

    # ------------------------------------------------------------

    def with_column(
        self,
        col: Union[
            bool,
            str,
            float,
            int,
            np.datetime64,
            FloatColumn,
            FloatColumnView,
            StringColumn,
            StringColumnView,
            BooleanColumnView,
        ],
        name: str,
        role: Optional[Role] = None,
        subroles: Optional[Union[Subrole, Iterable[str]]] = None,
        unit: str = "",
        time_formats: Optional[Iterable[str]] = None,
    ):
        """Returns a new [`View`][getml.data.View] that contains an additional column.

        Args:
            col:
                The column to be added.

            name:
                Name of the new column.

            role:
                Role of the new column. Must be from [`roles`][getml.data.roles].

            subroles:
                Subroles of the new column. Must be from [`subroles`][getml.data.subroles].

            unit:
                Unit of the column.

            time_formats:
                Formats to be used to parse the time stamps.

                This is only necessary, if an implicit conversion from
                a [`StringColumn`][getml.data.columns.StringColumn] to a time
                stamp is taking place.

                The formats are allowed to contain the following
                special characters:

                * %w - abbreviated weekday (Mon, Tue, ...)
                * %W - full weekday (Monday, Tuesday, ...)
                * %b - abbreviated month (Jan, Feb, ...)
                * %B - full month (January, February, ...)
                * %d - zero-padded day of month (01 .. 31)
                * %e - day of month (1 .. 31)
                * %f - space-padded day of month ( 1 .. 31)
                * %m - zero-padded month (01 .. 12)
                * %n - month (1 .. 12)
                * %o - space-padded month ( 1 .. 12)
                * %y - year without century (70)
                * %Y - year with century (1970)
                * %H - hour (00 .. 23)
                * %h - hour (00 .. 12)
                * %a - am/pm
                * %A - AM/PM
                * %M - minute (00 .. 59)
                * %S - second (00 .. 59)
                * %s - seconds and microseconds (equivalent to %S.%F)
                * %i - millisecond (000 .. 999)
                * %c - centisecond (0 .. 9)
                * %F - fractional seconds/microseconds (000000 - 999999)
                * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
                * %Z - time zone differential in RFC format (GMT or +NNNN)
                * %% - percent sign

        """
        col, role, subroles = _with_column(
            col, name, role, subroles, unit, time_formats
        )
        return View(
            base=self,
            added={
                "col_": col,
                "name_": name,
                "role_": role,
                "subroles_": subroles,
                "unit_": unit,
            },
        )

    # ------------------------------------------------------------

    def with_name(self, name: str) -> View:
        """Returns a new [`View`][getml.data.View] with a new name.

        Args:
            name:
                The name of the new view.

        Returns:
            A new view with the new name.
        """
        return View(base=self, name=name)

    # ------------------------------------------------------------

    def with_role(
        self,
        cols: Union[
            str,
            FloatColumn,
            StringColumn,
            Union[Iterable[str], List[FloatColumn], List[StringColumn]],
        ],
        role: Role,
        time_formats: Optional[Iterable[str]] = None,
    ):
        """Returns a new [`View`][getml.data.View] with modified roles.

        The difference between [`with_role`][getml.DataFrame.with_role] and
        [`set_role`][getml.DataFrame.set_role] is that
        [`with_role`][getml.DataFrame.with_role] returns a view that is lazily
        evaluated when needed whereas [`set_role`][getml.DataFrame.set_role]
        is an in-place operation. From a memory perspective, in-place operations
        like [`set_role`][getml.DataFrame.set_role] are preferable.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The `time_formats` argument is used to interpret time
        format string: `annotating_roles_time_stamp`. For more information on
        roles, please refer to the [User Guide][annotating-data].

        Args:
            cols:
                The columns or the names thereof.

            role:
                The role to be assigned.

            time_formats:
                Formats to be used to
                parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.
        """
        return _with_role(self, cols, role, time_formats)

    # ------------------------------------------------------------

    def with_subroles(
        self,
        cols: Union[
            str,
            FloatColumn,
            StringColumn,
            Union[Iterable[str], List[FloatColumn], List[StringColumn]],
        ],
        subroles: Union[Subrole, Iterable[str]],
        append: bool = True,
    ):
        """Returns a new view with one or several new subroles on one or more columns.

        The difference between [`with_subroles`][getml.DataFrame.with_subroles] and
        [`set_subroles`][getml.DataFrame.set_subroles] is that
        [`with_subroles`][getml.DataFrame.with_subroles] returns a view that is lazily
        evaluated when needed whereas [`set_subroles`][getml.DataFrame.set_subroles]
        is an in-place operation. From a memory perspective, in-place operations
        like [`set_subroles`][getml.DataFrame.set_subroles] are preferable.

        Args:
            cols:
                The columns or the names thereof.

            subroles:
                The subroles to be assigned.

            append:
                Whether you want to append the
                new subroles to the existing subroles.
        """
        return _with_subroles(self, cols, subroles, append)

    # ------------------------------------------------------------

    def with_unit(
        self,
        cols: Union[
            str,
            FloatColumn,
            StringColumn,
            Union[Iterable[str], List[FloatColumn], List[StringColumn]],
        ],
        unit: str,
        comparison_only: bool = False,
    ):
        """Returns a view that contains a new unit on one or more columns.

        The difference between [`with_unit`][getml.DataFrame.with_unit] and
        [`set_unit`][getml.DataFrame.set_unit] is that
        [`with_unit`][getml.DataFrame.with_unit] returns a view that is lazily
        evaluated when needed whereas [`set_unit`][getml.DataFrame.set_unit]
        is an in-place operation. From a memory perspective, in-place operations
        like [`set_unit`][getml.DataFrame.set_unit] are preferable.

        Args:
            cols:
                The columns or the names thereof.

            unit:
                The unit to be assigned.

            comparison_only:
                Whether you want the column to
                be used for comparison only. This means that the column can
                only be used in comparison to other columns of the same unit.

                An example might be a bank account number: The number in itself
                is hardly interesting, but it might be useful to know how often
                we have seen that same bank account number in another table.

                For more information on units, please refer to the
                [User Guide][annotating-data-units].
        """
        return _with_unit(self, cols, unit, comparison_only)
