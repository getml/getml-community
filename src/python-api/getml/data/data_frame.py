# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Handler for the data stored in the getML engine."""

import json
import numbers
import os
import shutil
from collections import namedtuple
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd  # type: ignore
import pyarrow as pa  # type: ignore

import getml.communication as comm
from getml import constants, database
from getml.constants import COMPARISON_ONLY, TIME_STAMP
from getml.database.helpers import _retrieve_temp_dir
from getml.utilities.formatting import _DataFrameFormatter

from . import roles as roles_
from .columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    arange,
    rowid,
)
from .columns.last_change import _last_change
from .helpers import (
    _check_if_exists,
    _empty_data_frame,
    _exists_in_memory,
    _get_column,
    _handle_cols,
    _is_non_empty_typed_list,
    _is_numerical_type,
    _is_typed_list,
    _make_default_slice,
    _modify_pandas_columns,
    _retrieve_urls,
    _send_numpy_array,
    _sniff_arrow,
    _sniff_csv,
    _sniff_db,
    _sniff_json,
    _sniff_pandas,
    _sniff_parquet,
    _sniff_s3,
    _to_arrow,
    _to_parquet,
    _to_pyspark,
    _transform_timestamps,
    _update_sniffed_roles,
    _with_column,
    _with_role,
    _with_subroles,
    _with_unit,
)
from .placeholder import Placeholder
from .roles_obj import Roles
from .view import View

# --------------------------------------------------------------------


class DataFrame:
    """Handler for the data stored in the getML engine.

    The :class:`~getml.DataFrame` class represents a data frame
    object in the getML engine but does not contain any actual data
    itself. To create such a data frame object, fill it with data via
    the Python API, and to retrieve a handler for it, you can use one
    of the :func:`~getml.DataFrame.from_csv`,
    :func:`~getml.DataFrame.from_db`,
    :func:`~getml.DataFrame.from_json`, or
    :func:`~getml.DataFrame.from_pandas` class methods. The
    :ref:`importing_data` section in the user guide explains the
    particularities of each of those flavors of the unified
    import interface.

    If the data frame object is already present in the engine -
    either in memory as a temporary object or on disk when
    :meth:`~getml.DataFrame.save` was called earlier -, the
    :func:`~getml.data.load_data_frame` function will create a new
    handler without altering the underlying data. For more information
    about the lifecycle of the data in the getML engine and its
    synchronization with the Python API please see the
    :ref:`corresponding user guide<the_getml_python_api_lifecycles>`.

    Args:
        name (str):
            Unique identifier used to link the handler with
            the underlying data frame object in the engine.

        roles (dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
            Maps the :mod:`~getml.data.roles` to the
            column names (see :meth:`~getml.DataFrame.colnames`).

            The `roles` dictionary is expected to have the following format

            .. code-block:: python

                roles = {getml.data.role.numeric: ["colname1", "colname2"],
                         getml.data.role.target: ["colname3"]}

            Otherwise, you can use the :class:`~getml.data.Roles` class.

    Examples:
        Creating a new data frame object in the getML engine and importing
        data is done by one the class functions
        :func:`~getml.DataFrame.from_csv`,
        :func:`~getml.DataFrame.from_db`,
        :func:`~getml.DataFrame.from_json`, or
        :func:`~getml.DataFrame.from_pandas`.

        .. code-block:: python

            random = numpy.random.RandomState(7263)

            table = pandas.DataFrame()
            table['column_01'] = random.randint(0, 10, 1000).astype(numpy.str)
            table['join_key'] = numpy.arange(1000)
            table['time_stamp'] = random.rand(1000)
            table['target'] = random.rand(1000)

            df_table = getml.DataFrame.from_pandas(table, name = 'table')

        In addition to creating a new data frame object in the getML
        engine and filling it with all the content of `table`, the
        :func:`~getml.DataFrame.from_pandas` function also
        returns a :class:`~getml.DataFrame` handler to the
        underlying data.

        You don't have to create the data frame objects anew for each
        session. You can use their :meth:`~getml.DataFrame.save`
        method to write them to disk, the
        :func:`~getml.data.list_data_frames` function to list all
        available objects in the engine, and
        :func:`~getml.data.load_data_frame` to create a
        :class:`~getml.DataFrame` handler for a data set already
        present in the getML engine (see
        :ref:`the_getml_python_api_lifecycles` for details).

        .. code-block:: python

            df_table.save()

            getml.data.list_data_frames()

            df_table_reloaded = getml.data.load_data_frame('table')

    Note:
        Although the Python API does not store the actual data itself,
        you can use the :meth:`~getml.DataFrame.to_csv`,
        :meth:`~getml.DataFrame.to_db`,
        :meth:`~getml.DataFrame.to_json`, and
        :meth:`~getml.DataFrame.to_pandas` methods to retrieve
        them.

    """

    _categorical_roles = roles_._categorical_roles
    _numerical_roles = roles_._numerical_roles
    _possible_keys = _categorical_roles + _numerical_roles

    def __init__(self, name, roles=None):
        # ------------------------------------------------------------

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        vars(self)["name"] = name

        # ------------------------------------------------------------

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        roles = roles or dict()

        if not isinstance(roles, dict):
            raise TypeError("'roles' must be dict or a getml.data.Roles object")

        for key, val in roles.items():
            if key not in self._possible_keys:
                msg = "'{}' is not a proper role and will be ignored\n"
                msg += "Possible roles are: {}"
                raise ValueError(msg.format(key, self._possible_keys))
            if not _is_typed_list(val, str):
                raise TypeError(
                    "'{}' must be None, an empty list, or a list of str.".format(key)
                )

        # ------------------------------------------------------------

        join_keys = roles.get("join_key", [])
        time_stamps = roles.get("time_stamp", [])
        categorical = roles.get("categorical", [])
        numerical = roles.get("numerical", [])
        targets = roles.get("target", [])
        text = roles.get("text", [])
        unused_floats = roles.get("unused_float", [])
        unused_strings = roles.get("unused_string", [])

        # ------------------------------------------------------------

        vars(self)["_categorical_columns"] = [
            StringColumn(name=cname, role=roles_.categorical, df_name=self.name)
            for cname in categorical
        ]

        vars(self)["_join_key_columns"] = [
            StringColumn(name=cname, role=roles_.join_key, df_name=self.name)
            for cname in join_keys
        ]

        vars(self)["_numerical_columns"] = [
            FloatColumn(name=cname, role=roles_.numerical, df_name=self.name)
            for cname in numerical
        ]

        vars(self)["_target_columns"] = [
            FloatColumn(name=cname, role=roles_.target, df_name=self.name)
            for cname in targets
        ]

        vars(self)["_text_columns"] = [
            StringColumn(name=cname, role=roles_.text, df_name=self.name)
            for cname in text
        ]

        vars(self)["_time_stamp_columns"] = [
            FloatColumn(name=cname, role=roles_.time_stamp, df_name=self.name)
            for cname in time_stamps
        ]

        vars(self)["_unused_float_columns"] = [
            FloatColumn(name=cname, role=roles_.unused_float, df_name=self.name)
            for cname in unused_floats
        ]

        vars(self)["_unused_string_columns"] = [
            StringColumn(name=cname, role=roles_.unused_string, df_name=self.name)
            for cname in unused_strings
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
        """Deletes the data frame from the getML engine.

        If called with the `mem_only` option set to True, the data
        frame corresponding to the handler represented by the current
        instance can be reloaded using the
        :meth:`~getml.DataFrame.load` method.

        Args:
            mem_only (bool, optional):
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

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return super().__getattribute__(name)

    # ------------------------------------------------------------

    def __getitem__(self, name):
        if isinstance(
            name,
            (numbers.Integral, slice, BooleanColumnView, FloatColumn, FloatColumnView),
        ):
            return self.where(index=name)

        if isinstance(name, list):
            not_in_colnames = set(name) - set(self.colnames)
            if not_in_colnames:
                raise KeyError(f"{list(not_in_colnames)} not found.")
            dropped = [col for col in self.colnames if col not in name]
            return View(base=self, dropped=dropped)

        col = _get_column(name, self._columns)

        if col is not None:
            return col

        raise KeyError("Column named '" + name + "' not found.")

    # ------------------------------------------------------------

    def _getml_deserialize(self) -> Dict[str, Any]:
        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame"
        cmd["name_"] = self.name

        return cmd

    # ----------------------------------------------------------------

    def __len__(self):
        return self.nrows()

    # ----------------------------------------------------------------

    def __repr__(self):
        if not _exists_in_memory(self.name):
            return _empty_data_frame()

        formatted = self._format()

        footer = self._collect_footer_data()

        return formatted._render_string(footer=footer)

    # ----------------------------------------------------------------

    def _repr_html_(self):
        return self.to_html()

    # ------------------------------------------------------------

    def __setattr__(self, name, value):
        if name in vars(self):
            vars(self)[name] = value
        else:
            self.add(value, name)

    # ------------------------------------------------------------

    def __setitem__(self, name, col):
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
                comm.engine_exception_handler(msg)

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
                comm.engine_exception_handler(msg)

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
            if _is_numerical_type(temp_df.dtypes[0]):
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
            self._set_subroles(name, subroles, append=False)

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
            comm.engine_exception_handler(msg)

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

    def _set_subroles(self, name: str, subroles: List[str], append: bool):
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

    def add(self, col, name, role=None, subroles=None, unit="", time_formats=None):
        """Adds a column to the current :class:`~getml.DataFrame`.

        Args:
            col (:mod:`~getml.column` or :mod:`numpy.ndarray`):
                The column or numpy.ndarray to be added.

            name (str):
                Name of the new column.

            role (str, optional):
                Role of the new column. Must be from :mod:`getml.data.roles`.

            subroles (str, List[str] or None, optional):
                Subroles of the new column. Must be from :mod:`getml.data.subroles`.

            unit (str, optional):
                Unit of the column.

            time_formats (str, optional):
                Formats to be used to parse the time stamps.

                This is only necessary, if an implicit conversion from
                a :class:`~getml.data.columns.StringColumn` to a time
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
    def _categorical_names(self):
        return [col.name for col in self._categorical_columns]

    # ------------------------------------------------------------

    @property
    def colnames(self):
        """
        List of the names of all columns.

        Returns:
            List[str]:
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
    def columns(self):
        """
        Alias for :meth:`~getml.DataFrame.colnames`.

        Returns:
            List[str]:
                List of the names of all columns.
        """
        return self.colnames

    # ------------------------------------------------------------

    def copy(self, name: str) -> "DataFrame":
        """
        Creates a deep copy of the data frame under a new name.

        Args:
            name (str):
                The name of the new data frame.

        Returns:
            :class:`~getml.DataFrame`:
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
        Permanently deletes the data frame. :meth:`delete` first unloads the data frame
        from memory and than deletes it from disk.
        """
        # ------------------------------------------------------------

        self._delete()

    # ------------------------------------------------------------

    def drop(self, cols):
        """Returns a new :class:`~getml.data.View` that has one or several columns removed.

        Args:
            cols (str, FloatColumn, StingColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.
        """

        names = _handle_cols(cols)

        if not _is_typed_list(names, str):
            raise TypeError("'cols' must be a string or a list of strings.")

        return View(base=self, dropped=names)

    # ------------------------------------------------------------

    def freeze(self):
        """Freezes the data frame.

        After you have frozen the data frame, the data frame is immutable
        and in-place operations are no longer possible. However, you can
        still create views. In other words, operations like
        :meth:`~getml.DataFrame.set_role` are not longer possible,
        but operations like :meth:`~getml.DataFrame.with_role` are.
        """
        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.freeze"
        cmd["name_"] = self.name
        comm.send(cmd)

    # ------------------------------------------------------------

    @classmethod
    def from_arrow(cls, table, name, roles=None, ignore=False, dry=False):
        """Create a DataFrame from an Arrow Table.

        This is one of the fastest way to get data into the
        getML engine.

        Args:
            table (:py:class:`pyarrow.Table`):
                The table to be read.

            name (str):
                Name of the data frame to be created.

            roles (dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.
        """

        # ------------------------------------------------------------

        if not isinstance(table, pa.Table):
            raise TypeError("'table' must be of type pyarrow.Table.")

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

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_arrow(table)

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_arrow(table=table, append=False)

    # --------------------------------------------------------------------

    @classmethod
    def from_csv(
        cls,
        fnames,
        name,
        num_lines_sniffed=1000,
        num_lines_read=0,
        quotechar='"',
        sep=",",
        skip=0,
        colnames=None,
        roles=None,
        ignore=False,
        dry=False,
        verbose=True,
    ) -> "DataFrame":
        """Create a DataFrame from CSV files.

        The getML engine will construct a data
        frame object in the engine, fill it with the data read from
        the CSV file(s), and return a corresponding
        :class:`~getml.DataFrame` handle.

        Args:
            fnames (List[str]):
                CSV file paths to be read.

            name (str):
                Name of the data frame to be created.

            num_lines_sniffed (int, optional):
                Number of lines analyzed by the sniffer.

            num_lines_read (int, optional):
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            quotechar (str, optional):
                The character used to wrap strings.

            sep (str, optional):
                The separator used for separating fields.

            skip (int, optional):
                Number of lines to skip at the beginning of each file.

            colnames(List[str] or None, optional): The first line of a CSV file
                usually contains the column names. When this is not the case,
                you need to explicitly pass them.

            roles(dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

            verbose (bool, optional):
                If True, when fnames are urls, the filenames are
                printed to stdout during the download.

        Returns:
            :class:`~getml.DataFrame`:

                Handler of the underlying data.

        Note:
            It is assumed that the first line of each CSV file
            contains a header with the column names.

        Examples:
            Let's assume you have two CSV files - *file1.csv* and
            *file2.csv* - in the current working directory. You can
            import their data into the getML engine using.

            >>> df_expd = data.DataFrame.from_csv(
                    ...     fnames=["file1.csv", "file2.csv"],
                    ...     name="MY DATA FRAME",
                    ...     sep=';',
                    ...     quotechar='"'
                    ... )

            However, the CSV format lacks type safety. If you want to
            build a reliable pipeline, it is a good idea
            to hard-code the roles:

                >>> roles = {"categorical": ["col1", "col2"], "target": ["col3"]}
            >>>
            >>> df_expd = data.DataFrame.from_csv(
                    ...         fnames=["file1.csv", "file2.csv"],
                    ...         name="MY DATA FRAME",
                    ...         sep=';',
                    ...         quotechar='"',
                    ...         roles=roles
                    ... )

            If you think that typing out all of the roles by hand is too
            cumbersome, you can use a dry run:

                >>> roles = data.DataFrame.from_csv(
                        ...         fnames=["file1.csv", "file2.csv"],
                        ...         name="MY DATA FRAME",
                        ...         sep=';',
                        ...         quotechar='"',
                        ...         dry=True
                        ... )

            This will return the roles dictionary it would have used. You
            can now hard-code this.

        """

        if not isinstance(fnames, list):
            fnames = [fnames]

        if not _is_non_empty_typed_list(fnames, str):
            raise TypeError("'fnames' must be either a str or a list of str.")

        if not isinstance(name, str):
            raise TypeError("'name' must be str.")

        if not isinstance(num_lines_sniffed, numbers.Real):
            raise TypeError("'num_lines_sniffed' must be a real number")

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

        if colnames is not None and not _is_non_empty_typed_list(colnames, str):
            raise TypeError(
                "'colnames' must be either be None or a non-empty list of str."
            )

        fnames = _retrieve_urls(fnames, verbose=verbose)

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_csv(
                fnames=fnames,
                num_lines_sniffed=int(num_lines_sniffed),
                quotechar=quotechar,
                sep=sep,
                skip=int(skip),
                colnames=colnames,
            )

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

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
        )

    # ------------------------------------------------------------

    @classmethod
    def from_db(
        cls, table_name, name=None, roles=None, ignore=False, dry=False, conn=None
    ):
        """Create a DataFrame from a table in a database.

        It will construct a data frame object in the engine, fill it
        with the data read from table `table_name` in the connected
        database (see :mod:`~getml.database`), and return a
        corresponding :class:`~getml.DataFrame` handle.

        Args:
            table_name (str):
                Name of the table to be read.

            name (str):
                Name of the data frame to be created. If not passed,
                then the *table_name* will be used.

            roles(dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

            conn (:class:`~getml.database.Connection`, optional):
                The database connection to be used.
                If you don't explicitly pass a connection, the engine
                will use the default connection.

        Returns:
            :class:`~getml.DataFrame`:

                Handler of the underlying data.

        Example:
            .. code-block:: python

                getml.database.connect_mysql(
                    host="relational.fit.cvut.cz",
                    port=3306,
                    dbname="financial",
                    user="guest",
                    password="relational"
                )

                loan = getml.DataFrame.from_db(
                    table_name='loan', name='data_frame_loan')

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

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_db(table_name, conn)

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_db(table_name=table_name, append=False, conn=conn)

    # --------------------------------------------------------------------

    @classmethod
    def from_dict(
        cls, data: Dict[str, List[Any]], name: str, roles=None, ignore=False, dry=False
    ):
        """Create a new DataFrame from a dict

        Args:
            data (dict):
                The dict containing the data.
                The data should be in the following format:

                .. code-block:: python

                    data = {'col1': [1.0, 2.0, 1.0], 'col2': ['A', 'B', 'C']}

            name (str):
                Name of the data frame to be created.

            roles(dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

        Returns:
            :class:`~getml.DataFrame`:

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

    @classmethod
    def from_json(cls, json_str, name, roles=None, ignore=False, dry=False):
        """Create a new DataFrame from a JSON string.

        It will construct a data frame object in the engine, fill it
        with the data read from the JSON string, and return a
        corresponding :class:`~getml.DataFrame` handle.

        Args:
            json_str (str):
                The JSON string containing the data.
                The json_str should be in the following format:

                .. code-block:: python

                    json_str = "{'col1': [1.0, 2.0, 1.0], 'col2': ['A', 'B', 'C']}"

            name (str):
                Name of the data frame to be created.

            roles(dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

        Returns:
            :class:`~getml.DataFrame`:

        Returns:
            :class:`~getml.data.DataFrame`: Handler of the underlying data.
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

    @classmethod
    def from_pandas(cls, pandas_df, name, roles=None, ignore=False, dry=False):
        """Create a DataFrame from a :py:class:`pandas.DataFrame`.

        It will construct a data frame object in the engine, fill it
        with the data read from the :py:class:`pandas.DataFrame`, and
        return a corresponding :class:`~getml.DataFrame` handle.

        Args:
            pandas_df (:py:class:`pandas.DataFrame`):
                The table to be read.

            name (str):
                Name of the data frame to be created.

            roles (dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.
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

        pandas_df_modified = _modify_pandas_columns(pandas_df)

        # ------------------------------------------------------------

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_pandas(pandas_df_modified)

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_pandas(pandas_df=pandas_df_modified, append=False)

    # --------------------------------------------------------------------

    @classmethod
    def from_parquet(cls, fname, name, roles=None, ignore=False, dry=False):
        """Create a DataFrame from parquet files.

        This is one of the fastest way to get data into the
        getML engine.

        Args:
            fname (str):
                The path of the parquet file to be read.

            name (str):
                Name of the data frame to be created.

            roles (dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.
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

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_parquet(fname)

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_parquet(fname=fname, append=False)

    # --------------------------------------------------------------------

    @classmethod
    def from_pyspark(cls, spark_df, name, roles=None, ignore=False, dry=False):
        """Create a DataFrame from a :py:class:`pyspark.sql.DataFrame`.

        It will construct a data frame object in the engine, fill it
        with the data read from the :py:class:`pyspark.sql.DataFrame`, and
        return a corresponding :class:`~getml.DataFrame` handle.

        Args:
            spark_df (:py:class:`pyspark.sql.DataFrame`):
                The table to be read.

            name (str):
                Name of the data frame to be created.

            roles (dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

        Returns:
            :class:`~getml.DataFrame`:

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

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            head = spark_df.limit(2).toPandas()

            sniffed_roles = _sniff_pandas(head)

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

        if dry:
            return roles

        data_frame = cls(name, roles)

        return data_frame.read_pyspark(spark_df=spark_df, append=False)

    # ------------------------------------------------------------

    @classmethod
    def from_s3(
        cls,
        bucket: str,
        keys: List[str],
        region: str,
        name: str,
        num_lines_sniffed=1000,
        num_lines_read=0,
        sep=",",
        skip=0,
        colnames=None,
        roles=None,
        ignore=False,
        dry=False,
    ) -> "DataFrame":
        """Create a DataFrame from CSV files located in an S3 bucket.

        This classmethod will construct a data
        frame object in the engine, fill it with the data read from
        the CSV file(s), and return a corresponding
        :class:`~getml.DataFrame` handle.

        Args:
            bucket (str):
                The bucket from which to read the files.

            keys (List[str]):
                The list of keys (files in the bucket) to be read.

            region (str):
                The region in which the bucket is located.

            name (str):
                Name of the data frame to be created.

            num_lines_sniffed (int, optional):
                Number of lines analyzed by the sniffer.

            num_lines_read (int, optional):
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            sep (str, optional):
                The separator used for separating fields.

            skip (int, optional):
                Number of lines to skip at the beginning of each file.

            colnames(List[str] or None, optional):
                The first line of a CSV file
                usually contains the column names. When this is not the case,
                you need to explicitly pass them.

            roles(dict[str, List[str]] or :class:`~getml.data.Roles`, optional):
                Maps the :mod:`~getml.data.roles` to the
                column names (see :meth:`~getml.DataFrame.colnames`).

                The `roles` dictionary is expected to have the following format:

                .. code-block:: python

                    roles = {getml.data.role.numeric: ["colname1", "colname2"],
                             getml.data.role.target: ["colname3"]}

                Otherwise, you can use the :class:`~getml.data.Roles` class.

            ignore (bool, optional):
                Only relevant when roles is not None.
                Determines what you want to do with any colnames not
                mentioned in roles. Do you want to ignore them (True)
                or read them in as unused columns (False)?

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

        Returns:
            :class:`~getml.DataFrame`:

                Handler of the underlying data.

        Example:
            Let's assume you have two CSV files - *file1.csv* and
            *file2.csv* - in the bucket. You can
            import their data into the getML engine using the following
            commands:

            >>> getml.engine.set_s3_access_key_id("YOUR-ACCESS-KEY-ID")
            >>>
            >>> getml.engine.set_s3_secret_access_key("YOUR-SECRET-ACCESS-KEY")
            >>>
            >>> data_frame_expd = data.DataFrame.from_s3(
                    ...         bucket="your-bucket-name",
                    ...         keys=["file1.csv", "file2.csv"],
                    ...         region="us-east-2",
                    ...         name="MY DATA FRAME",
                    ...         sep=';'
                    ... )

            You can also set the access credential as environment variables
            before you launch the getML engine.

            Also refer to the documention on :meth:`~getml.DataFrame.from_csv`
            for further information on overriding the CSV sniffer for greater
            type safety.

        Note:
            Not supported in the getML community edition.
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

        roles = roles.to_dict() if isinstance(roles, Roles) else roles

        if roles is None or not ignore:
            sniffed_roles = _sniff_s3(
                bucket=bucket,
                keys=keys,
                region=region,
                num_lines_sniffed=int(num_lines_sniffed),
                sep=sep,
                skip=int(skip),
                colnames=colnames,
            )

            if roles is None:
                roles = sniffed_roles
            else:
                roles = _update_sniffed_roles(sniffed_roles, roles)

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

    @classmethod
    def from_view(
        cls,
        view,
        name,
        dry=False,
    ):
        """Create a DataFrame from a :class:`~getml.data.View`.

        This classmethod will construct a data
        frame object in the engine, fill it with the data read from
        the :class:`~getml.data.View`, and return a corresponding
        :class:`~getml.DataFrame` handle.

        Args:
            view (:class:`~getml.data.View`):
                The view from which we want to read the data.

            name (str):
                Name of the data frame to be created.

            dry (bool, optional):
                If set to True, then the data
                will not actually be read. Instead, the method will only
                return the roles it would have used. This can be used
                to hard-code roles when setting up a pipeline.

        Returns:
            :class:`~getml.DataFrame`:
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

        data_frame = cls(name)

        # ------------------------------------------------------------

        return data_frame.read_view(view=view, append=False)

    # ------------------------------------------------------------

    @property
    def _join_key_names(self):
        return [col.name for col in self._join_key_columns]

    # ------------------------------------------------------------

    @property
    def last_change(self) -> str:
        """
        A string describing the last time this data frame has been changed.
        """
        return _last_change(self.name)

    # ------------------------------------------------------------

    def load(self) -> "DataFrame":
        """Loads saved data from disk.

        The data frame object holding the same name as the current
        :class:`~getml.DataFrame` instance will be loaded from
        disk into the getML engine and updates the current handler
        using :meth:`~getml.DataFrame.refresh`.

        Examples:
            First, we have to create and imporimport data sets.

            .. code-block:: python

                d, _ = getml.datasets.make_numerical(population_name = 'test')
                getml.data.list_data_frames()

            In the output of :func:`~getml.data.list_data_frames` we
            can find our underlying data frame object 'test' listed
            under the 'in_memory' key (it was created and imported by
            :func:`~getml.datasets.make_numerical`). This means the
            getML engine does only hold it in memory (RAM) yet and we
            still have to :meth:`~getml.DataFrame.save` it to
            disk in order to :meth:`~getml.DataFrame.load` it
            again or to prevent any loss of information between
            different sessions.

            .. code-block:: python

                d.save()
                getml.data.list_data_frames()
                d2 = getml.DataFrame(name = 'test').load()

        Returns:
            :class:`~getml.DataFrame`:
                Updated handle the underlying data frame in the getML
                engine.

        Note:
            When invoking :meth:`~getml.DataFrame.load` all
            changes of the underlying data frame object that took
            place after the last call to the
            :meth:`~getml.DataFrame.save` method will be
            lost. Thus, this method  enables you to undo changes
            applied to the :class:`~getml.DataFrame`.

            .. code-block:: python

                d, _ = getml.datasets.make_numerical()
                d.save()

                # Accidental change we want to undo
                d.rm('column_01')

                d.load()

            If :meth:`~getml.DataFrame.save` hasn't be called
            on the current instance yet or it wasn't stored to disk in
            a previous session, :meth:`~getml.DataFrame.load`
            will throw an exception

                File or directory '../projects/X/data/Y/' not found!

            Alternatively, :func:`~getml.data.load_data_frame`
            offers an easier way of creating
            :class:`~getml.DataFrame` handlers to data in the
            getML engine.

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
        Convencience wrapper that returns the memory usage in MB.
        """
        return self.nbytes() / 1e06

    # ------------------------------------------------------------

    @property
    def _monitor_url(self) -> Optional[str]:
        """
        A link to the data frame in the getML monitor.
        """
        url = comm._monitor_url()
        return (
            url + "getdataframe/" + comm._get_project_name() + "/" + self.name + "/"
            if url
            else None
        )

    # ------------------------------------------------------------

    def nbytes(self):
        """Size of the data stored in the underlying data frame in the getML
        engine.

        Returns:
            :py:class:`numpy.uint64`:
                Size of the underlying object in bytes.

        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.nbytes"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                sock.close()
                comm.engine_exception_handler(msg)
            nbytes = comm.recv_string(sock)

        return np.uint64(nbytes)

    # ------------------------------------------------------------

    def ncols(self):
        """
        Number of columns in the current instance.

        Returns:
            int:
                Overall number of columns
        """
        return len(self.colnames)

    # ------------------------------------------------------------

    def nrows(self):
        """
        Number of rows in the current instance.
        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.nrows"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                sock.close()
                comm.engine_exception_handler(msg)
            nrows = comm.recv_string(sock)

        return int(nrows)

    # ------------------------------------------------------------

    @property
    def _numerical_names(self):
        return [col.name for col in self._numerical_columns]

    # --------------------------------------------------------------------------

    def read_arrow(self, table, append=False):
        """Uploads a :class:`pyarrow.Table`.

        Replaces the actual content of the underlying data frame in
        the getML engine with `table`.

        Args:
            table (:class:`pyarrow.Table`):
                Data the underlying data frame object in the getML
                engine should obtain.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML engine, should the content in
                `query` be appended or replace the existing data?

        Returns:
            :class:`~getml.DataFrame`:
                Current instance.

        Note:
            For columns containing :class:`pandas.Timestamp` there can
            be small inconsistencies in the order to microseconds
            when sending the data to the getML engine. This is due to
            the way the underlying information is stored.
        """

        # ------------------------------------------------------------

        if not isinstance(table, pa.Table):
            raise TypeError("'table' must be of type pyarrow.Table.")

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

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.from_arrow"
        cmd["name_"] = self.name

        cmd["append_"] = append

        cmd["categorical_"] = self._categorical_names
        cmd["join_keys_"] = self._join_key_names
        cmd["numerical_"] = self._numerical_names
        cmd["targets_"] = self._target_names
        cmd["text_"] = self._text_names
        cmd["time_stamps_"] = self._time_stamp_names
        cmd["unused_floats_"] = self._unused_float_names
        cmd["unused_strings_"] = self._unused_string_names

        with comm.send_and_get_socket(cmd) as sock:
            with sock.makefile(mode="wb") as sink:
                batches = table.to_batches()
                with pa.ipc.new_stream(sink, table.schema) as writer:
                    for batch in batches:
                        writer.write_batch(batch)
            msg = comm.recv_string(sock)

        if msg != "Success!":
            comm.engine_exception_handler(msg)

        return self.refresh()

    # --------------------------------------------------------------------------

    def read_csv(
        self,
        fnames,
        append=False,
        quotechar='"',
        sep=",",
        num_lines_read=0,
        skip=0,
        colnames=None,
        time_formats=None,
        verbose=True,
    ) -> "DataFrame":
        """Read CSV files.

        It is assumed that the first line of each CSV file contains a
        header with the column names.

        Args:
            fnames (List[str]):
                CSV file paths to be read.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of of
                the CSV files in `fnames` be appended or replace the
                existing data?

            quotechar (str, optional):
                The character used to wrap strings.

            sep (str, optional):
                The separator used for separating fields.

            num_lines_read (int, optional):
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            skip (int, optional):
                Number of lines to skip at the beginning of each file.

            colnames(List[str] or None, optional):
                The first line of a CSV file
                usually contains the column names.
                When this is not the case, you need to explicitly pass them.

            time_formats (List[str], optional):
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

            verbose (bool, optional):
                If True, when fnames are urls, the filenames are printed to
                stdout during the download.

        Returns:
            :class:`~getml.DataFrame`:
                Handler of the underlying data.

        """

        time_formats = time_formats or constants.TIME_FORMATS

        if not isinstance(fnames, list):
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

        if colnames is not None and not _is_non_empty_typed_list(colnames, str):
            raise TypeError(
                "'colnames' must be either be None or a non-empty list of str."
            )

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

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.read_csv"
        cmd["name_"] = self.name

        cmd["fnames_"] = fnames_

        cmd["append_"] = append
        cmd["num_lines_read_"] = num_lines_read
        cmd["quotechar_"] = quotechar
        cmd["sep_"] = sep
        cmd["skip_"] = skip
        cmd["time_formats_"] = time_formats

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

    # --------------------------------------------------------------------------

    def read_json(self, json_str, append=False, time_formats=None):
        """Fill from JSON

        Fills the data frame with data from a JSON string.

        Args:

            json_str (str):
                The JSON string containing the data.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                `json_str` be appended or replace the existing data?

            time_formats (List[str], optional):
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
            :class:`~getml.DataFrame`:
                Handler of the underlying data.

        Note:
            This does not support NaN values. If you want support for NaN,
            use :meth:`~getml.DataFrame.from_json` instead.
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
            comm.engine_exception_handler(msg)

        return self

    # --------------------------------------------------------------------------

    def read_parquet(
        self,
        fname: str,
        append: bool = False,
        verbose: bool = True,
    ) -> "DataFrame":
        """Read a parquet file.

        Args:
            fname (str):
                The filepath of the parquet file to be read.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of of
                the CSV files in `fnames` be appended or replace the
                existing data?
        """

        if not isinstance(fname, str):
            raise TypeError("'fname' must be str.")

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        if self.ncols() == 0:
            raise Exception(
                """Reading data is only possible in a DataFrame with more than
                zero columns. You can pre-define columns during
                initialization of the DataFrame or use the classmethod
                from_parquet(...)."""
            )

        fname_ = _retrieve_urls([fname], verbose)[0]

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.read_parquet"
        cmd["name_"] = self.name

        cmd["fname_"] = fname_
        cmd["append_"] = append

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

    # --------------------------------------------------------------------------

    def read_s3(
        self,
        bucket: str,
        keys: List[str],
        region: str,
        append: bool = False,
        sep: str = ",",
        num_lines_read: int = 0,
        skip: int = 0,
        colnames: Optional[List[str]] = None,
        time_formats: Optional[List[str]] = None,
    ):
        """Read CSV files from an S3 bucket.

        It is assumed that the first line of each CSV file contains a
        header with the column names.

        Args:
            bucket (str):
                The bucket from which to read the files.

            keys (List[str]):
                The list of keys (files in the bucket) to be read.

            region (str):
                The region in which the bucket is located.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of of
                the CSV files in `fnames` be appended or replace the
                existing data?

            sep (str, optional):
                The separator used for separating fields.

            num_lines_read (int, optional):
                Number of lines read from each file.
                Set to 0 to read in the entire file.

            skip (int, optional):
                Number of lines to skip at the beginning of each file.

            colnames(List[str] or None, optional):
                The first line of a CSV file
                usually contains the column names.
                When this is not the case, you need to explicitly pass them.

            time_formats (List[str], optional):
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
            :class:`~getml.DataFrame`:
                Handler of the underlying data.

        Note:
            Not supported in the getML community edition.
        """

        time_formats = time_formats or constants.TIME_FORMATS

        if not isinstance(keys, list):
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
    ) -> "DataFrame":
        """Read the data from a :class:`~getml.data.View`.

        Args:
            view (:class:`~getml.data.View`):
                The view to read.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of of
                the CSV files in `fnames` be appended or replace the
                existing data?

        Returns:
            :class:`~getml.DataFrame`:
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

    def read_db(self, table_name: str, append: bool = False, conn=None) -> "DataFrame":
        """
        Fill from Database.

        The DataFrame will be filled from a table in the database.

        Args:
            table_name(str):
                Table from which we want to retrieve the data.

            append(bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML, should the content of
                `table_name` be appended or replace the existing data?

            conn (:class:`~getml.database.Connection`, optional):
                The database connection to be used.
                If you don't explicitly pass a connection,
                the engine will use the default connection.

        Returns:
            :class:`~getml.DataFrame`:
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

    def read_pandas(self, pandas_df: pd.DataFrame, append: bool = False) -> "DataFrame":
        """Uploads a :class:`pandas.DataFrame`.

        Replaces the actual content of the underlying data frame in
        the getML engine with `pandas_df`.

        Args:
            pandas_df (:class:`pandas.DataFrame`):
                Data the underlying data frame object in the getML
                engine should obtain.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML engine, should the content in
                `query` be appended or replace the existing data?

        Note:
            For columns containing :class:`pandas.Timestamp` there can
            occur small inconsistencies in the order to microseconds
            when sending the data to the getML engine. This is due to
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

        table = pa.Table.from_pandas(_modify_pandas_columns(pandas_df))

        return self.read_arrow(table, append=append)

    # --------------------------------------------------------------------------

    def read_pyspark(self, spark_df, append: bool = False) -> "DataFrame":
        """Uploads a :py:class:`pyspark.sql.DataFrame`.

        Replaces the actual content of the underlying data frame in
        the getML engine with `pandas_df`.

        Args:
            spark_df (:py:class:`pyspark.sql.DataFrame`):
                Data the underlying data frame object in the getML
                engine should obtain.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML engine, should the content in
                `query` be appended or replace the existing data?
        """

        if not isinstance(append, bool):
            raise TypeError("'append' must be bool.")

        temp_dir = _retrieve_temp_dir()
        os.makedirs(temp_dir, exist_ok=True)
        path = os.path.join(temp_dir, self.name)
        spark_df.write.mode("overwrite").parquet(path)

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

    def read_query(self, query: str, append: bool = False, conn=None) -> "DataFrame":
        """Fill from query

        Fills the data frame with data from a table in the database.

        Args:
            query (str):
                The query used to retrieve the data.

            append (bool, optional):
                If a data frame object holding the same ``name`` is
                already present in the getML engine, should the content in
                `query` be appended or replace the existing data?

            conn (:class:`~getml.database.Connection`, optional):
                The database connection to be used.
                If you don't explicitly pass a connection,
                the engine will use the default connection.

        Returns:
            :class:`~getml.DataFrame`:
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

    def refresh(self) -> "DataFrame":
        """Aligns meta-information of the current instance with the
        corresponding data frame in the getML engine.

        This method can be used to avoid encoding conflicts. Note that
        :meth:`~getml.DataFrame.load` as well as several other
        methods automatically call :meth:`~getml.DataFrame.refresh`.

        Returns:
            :class:`~getml.DataFrame`:

                Updated handle the underlying data frame in the getML
                engine.

        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = "DataFrame.refresh"
        cmd["name_"] = self.name

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)

        if msg[0] != "{":
            comm.engine_exception_handler(msg)

        roles = json.loads(msg)

        self.__init__(name=self.name, roles=roles)  # type: ignore

        return self

    # ------------------------------------------------------------

    @property
    def roles(self):
        """
        The roles of the columns included
        in this DataFrame.
        """
        return Roles(
            categorical=self._categorical_names,
            join_key=self._join_key_names,
            numerical=self._numerical_names,
            target=self._target_names,
            text=self._text_names,
            time_stamp=self._time_stamp_names,
            unused_float=self._unused_float_names,
            unused_string=self._unused_string_names,
        )

    # ------------------------------------------------------------

    def remove_subroles(self, cols):
        """Removes all :mod:`~getml.data.subroles` from one or more columns.

        Args:
            columns (str, FloatColumn, StringColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.
        """

        names = _handle_cols(cols)

        for name in names:
            self._set_subroles(name, subroles=[], append=False)

        self.refresh()

    # ------------------------------------------------------------

    def remove_unit(self, cols):
        """Removes the unit from one or more columns.

        Args:
            columns (str, FloatColumn, StringColumn, or List[str, FloatColumn, StringColumn]):
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

    def save(self) -> "DataFrame":
        """Writes the underlying data in the getML engine to disk.

        Returns:
            :class:`~getml.DataFrame`:
                The current instance.

        """

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.save"
        cmd["name_"] = self.name

        comm.send(cmd)

        return self

    # ------------------------------------------------------------

    def set_role(self, cols, role, time_formats=None):
        """Assigns a new role to one or more columns.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The :code:`time_formats` argument is used to interpret :ref:`time
        format string <annotating_roles_time_stamp>`. For more information on
        roles, please refer to the :ref:`user guide <annotating>`.

        Args:
            columns (str, FloatColumn, StringColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names of the columns.

            role (str):
                The role to be assigned.

            time_formats (str or List[str], optional):
                Formats to be used to parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.

        Example:
            .. code-block:: python

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

            .. code-block:: pycon

                | date                        | animal      | votes     |
                | time stamp                  | categorical | numerical |
                ---------------------------------------------------------
                | 2019-06-04T00:00:00.000000Z | hawk        | 12341     |
                | 2019-03-01T00:00:00.000000Z | parrot      | 5127      |
                | 2018-12-24T00:00:00.000000Z | goose       | 65311     |
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

        if role not in self._possible_keys:
            raise ValueError(
                "'role' must be one of the following values: "
                + str(self._possible_keys)
            )

        # ------------------------------------------------------------

        for name in names:
            if self[name].role != role:
                self._set_role(name, role, time_formats)

        # ------------------------------------------------------------

        self.refresh()

    # ------------------------------------------------------------

    def set_subroles(self, cols, subroles, append=True):
        """Assigns one or several new :mod:`~getml.data.subroles` to one or more columns.

        Args:
            columns (str, FloatColumn, StringColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.

            subroles (str or List[str]):
                The subroles to be assigned.
                Must be from :mod:`~getml.data.subroles`.

            append (bool, optional):
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
            self._set_subroles(name, subroles, append)

        self.refresh()

    # ------------------------------------------------------------

    def set_unit(self, cols, unit, comparison_only=False):
        """Assigns a new unit to one or more columns.

        Args:
            columns (str, FloatColumn, StringColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.

            unit (str):
                The unit to be assigned.

            comparison_only (bool):
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

    def to_arrow(self):
        """Creates a :py:class:`pyarrow.Table` from the current instance.

        Loads the underlying data from the getML engine and constructs
        a :class:`pyarrow.Table`.

        Returns:
            :class:`pyarrow.Table`:
                Pyarrow equivalent of the current instance including
                its underlying data.
        """
        return _to_arrow(self)

    # ------------------------------------------------------------

    def to_csv(
        self, fname: str, quotechar: str = '"', sep: str = ",", batch_size: int = 0
    ):
        """
        Writes the underlying data into a newly created CSV file.

        Args:
            fname (str):
                The name of the CSV file.
                The ending ".csv" and an optional batch number will
                be added automatically.

            quotechar (str, optional):
                The character used to wrap strings.

            sep (str, optional):
                The character used for separating fields.

            batch_size(int, optional):
                Maximum number of lines per file. Set to 0 to read
                the entire data frame into a single file.
        """

        self.refresh()

        if not isinstance(fname, str):
            raise TypeError("'fname' must be of type str")

        if not isinstance(quotechar, str):
            raise TypeError("'quotechar' must be of type str")

        if not isinstance(sep, str):
            raise TypeError("'sep' must be of type str")

        if not isinstance(batch_size, numbers.Real):
            raise TypeError("'batch_size' must be a real number")

        fname_ = os.path.abspath(fname)

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "DataFrame.to_csv"
        cmd["name_"] = self.name

        cmd["fname_"] = fname_
        cmd["quotechar_"] = quotechar
        cmd["sep_"] = sep
        cmd["batch_size_"] = batch_size

        comm.send(cmd)

    # ------------------------------------------------------------

    def to_db(self, table_name, conn=None):
        """Writes the underlying data into a newly created table in the
        database.

        Args:
            table_name (str):
                Name of the table to be created.

                If a table of that name already exists, it will be
                replaced.

            conn (:class:`~getml.database.Connection`, optional):
                The database connection to be used.
                If you don't explicitly pass a connection,
                the engine will use the default connection.
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

    def to_html(self, max_rows=10):
        """
        Represents the data frame in HTML format, optimized for an
        iPython notebook.

        Args:
            max_rows (int):
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

        Loads the underlying data from the getML engine and constructs
        a JSON string.
        """
        return self.to_pandas().to_json()

    # ----------------------------------------------------------------

    def to_pandas(self):
        """Creates a :py:class:`pandas.DataFrame` from the current instance.

        Loads the underlying data from the getML engine and constructs
        a :class:`pandas.DataFrame`.

        Returns:
            :class:`pandas.DataFrame`:
                Pandas equivalent of the current instance including
                its underlying data.

        """
        return _to_arrow(self).to_pandas()

    # ------------------------------------------------------------

    def to_parquet(self, fname, compression="snappy"):
        """
        Writes the underlying data into a newly created parquet file.

        Args:
            fname (str):
                The name of the parquet file.
                The ending ".parquet" will be added automatically.

            compression (str):
                The compression format to use.
                Supported values are "brotli", "gzip", "lz4", "snappy", "zstd"
        """
        _to_parquet(self, fname, compression)

    # ----------------------------------------------------------------

    def to_placeholder(self, name=None):
        """Generates a :class:`~getml.data.Placeholder` from the
        current :class:`~getml.DataFrame`.

        Args:
            name (str, optional):
                The name of the placeholder. If no
                name is passed, then the name of the placeholder will
                be identical to the name of the current data frame.

        Returns:
            :class:`~getml.data.Placeholder`:
                A placeholder with the same name as this data frame.


        """
        self.refresh()
        return Placeholder(name=name or self.name, roles=self.roles)

    # ----------------------------------------------------------------

    def to_pyspark(self, spark, name=None):
        """Creates a :py:class:`pyspark.sql.DataFrame` from the current instance.

        Loads the underlying data from the getML engine and constructs
        a :class:`pyspark.sql.DataFrame`.

        Args:
            spark (:py:class:`pyspark.sql.SparkSession`):
                The pyspark session in which you want to
                create the data frame.

            name (str or None):
                The name of the temporary view to be created on top
                of the :py:class:`pyspark.sql.DataFrame`,
                with which it can be referred to
                in Spark SQL (refer to
                :py:meth:`pyspark.sql.DataFrame.createOrReplaceTempView`).
                If none is passed, then the name of this
                :class:`getml.DataFrame` will be used.

        Returns:
            :py:class:`pyspark.sql.DataFrame`:
                Pyspark equivalent of the current instance including
                its underlying data.

        """
        return _to_pyspark(self, name, spark)

    # ------------------------------------------------------------

    def to_s3(self, bucket: str, key: str, region: str, sep=",", batch_size=50000):
        """
        Writes the underlying data into a newly created CSV file
        located in an S3 bucket.

        NOTE THAT S3 IS NOT SUPPORTED ON WINDOWS.

        Args:
            bucket (str):
                The bucket from which to read the files.

            key (str):
                The key in the S3 bucket in which you want to
                write the output. The ending ".csv" and an optional
                batch number will be added automatically.

            region (str):
                The region in which the bucket is located.

            sep (str, optional):
                The character used for separating fields.

            batch_size(int, optional):
                Maximum number of lines per file. Set to 0 to read
                the entire data frame into a single file.

        Example:
            >>> getml.engine.set_s3_access_key_id("YOUR-ACCESS-KEY-ID")
            >>>
            >>> getml.engine.set_s3_secret_access_key("YOUR-SECRET-ACCESS-KEY")
            >>>
            >>> your_df.to_s3(
                    ...     bucket="your-bucket-name",
                    ...     key="filename-on-s3",
                    ...     region="us-east-2",
                    ...     sep=';'
                    ... )
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

        Creates a new :class:`~getml.data.View` as a
        subselection of the current instance.

        Args:
            index (int, slice, :class:`~getml.data.columns.BooleanColumnView` or :class:`~getml.data.columns.FloatColumnView` or :class:`~getml.data.columns.FloatColumn`):
                Indicates the rows you want to select.

        Example:

            Generate example data:

            .. code-block:: python

                data = dict(
                    fruit=["banana", "apple", "cherry", "cherry", "melon", "pineapple"],
                    price=[2.4, 3.0, 1.2, 1.4, 3.4, 3.4],
                    join_key=["0", "1", "2", "2", "3", "3"])

                fruits = getml.DataFrame.from_dict(data, name="fruits",
                roles={"categorical": ["fruit"], "join_key": ["join_key"], "numerical": ["price"]})

                fruits

            .. code-block:: pycon

                | join_key | fruit       | price     |
                | join key | categorical | numerical |
                --------------------------------------
                | 0        | banana      | 2.4       |
                | 1        | apple       | 3         |
                | 2        | cherry      | 1.2       |
                | 2        | cherry      | 1.4       |
                | 3        | melon       | 3.4       |
                | 3        | pineapple   | 3.4       |

            Apply where condition. This creates a new DataFrame called "cherries":

            .. code-block:: python

                cherries = fruits.where(
                    fruits["fruit"] == "cherry")

                cherries

            .. code-block:: pycon

                | join_key | fruit       | price     |
                | join key | categorical | numerical |
                --------------------------------------
                | 2        | cherry      | 1.2       |
                | 2        | cherry      | 1.4       |
        """
        if isinstance(index, numbers.Integral):
            index = index if int(index) > 0 else len(self) + index
            selector = arange(int(index), int(index) + 1)
            return View(base=self, subselection=selector)

        if isinstance(index, slice):
            start, stop, _ = _make_default_slice(index, len(self))
            selector = arange(start, stop, index.step)
            return View(base=self, subselection=selector)

        if isinstance(index, (BooleanColumnView, FloatColumn, FloatColumnView)):
            return View(base=self, subselection=index)

        raise TypeError("Unsupported type for a subselection: " + type(index).__name__)

    # ------------------------------------------------------------

    def with_column(
        self, col, name, role=None, subroles=None, unit="", time_formats=None
    ):
        """Returns a new :class:`~getml.data.View` that contains an additional column.

        Args:
            col (:mod:`~getml.columns`):
                The column to be added.

            name (str):
                Name of the new column.

            role (str, optional):
                Role of the new column. Must be from :mod:`getml.data.roles`.

            subroles (str, List[str] or None, optional):
                Subroles of the new column. Must be from :mod:`getml.data.subroles`.

            unit (str, optional):
                Unit of the column.

            time_formats (str, optional):
                Formats to be used to parse the time stamps.

                This is only necessary, if an implicit conversion from
                a :class:`~getml.data.columns.StringColumn` to a time
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

    def with_name(self, name):
        """Returns a new :class:`~getml.data.View` with a new name.

        Args:
            name (str):
                The name of the new view.
        """
        return View(base=self, name=name)

    # ------------------------------------------------------------

    def with_role(self, cols, role, time_formats=None):
        """Returns a new :class:`~getml.data.View` with modified roles.

        The difference between :meth:`~getml.DataFrame.with_role` and
        :meth:`~getml.DataFrame.set_role` is that
        :meth:`~getml.DataFrame.with_role` returns a view that is lazily
        evaluated when needed whereas :meth:`~getml.DataFrame.set_role`
        is an in-place operation. From a memory perspective, in-place operations
        like :meth:`~getml.DataFrame.set_role` are preferable.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The :code:`time_formats` argument is used to interpret :ref:`time
        format string <annotating_roles_time_stamp>`. For more information on
        roles, please refer to the :ref:`user guide <annotating>`.

        Args:
            cols (str, FloatColumn, StingColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.

            role (str):
                The role to be assigned.

            time_formats (str or List[str], optional):
                Formats to be used to
                parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.
        """
        return _with_role(self, cols, role, time_formats)

    # ------------------------------------------------------------

    def with_subroles(self, cols, subroles, append=True):
        """Returns a new view with one or several new subroles on one or more columns.

        The difference between :meth:`~getml.DataFrame.with_subroles` and
        :meth:`~getml.DataFrame.set_subroles` is that
        :meth:`~getml.DataFrame.with_subroles` returns a view that is lazily
        evaluated when needed whereas :meth:`~getml.DataFrame.set_subroles`
        is an in-place operation. From a memory perspective, in-place operations
        like :meth:`~getml.DataFrame.set_subroles` are preferable.

        Args:
            cols (str, FloatColumn, StingColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.

            subroles (str or List[str]):
                The subroles to be assigned.

            append (bool, optional):
                Whether you want to append the
                new subroles to the existing subroles.
        """
        return _with_subroles(self, cols, subroles, append)

    # ------------------------------------------------------------

    def with_unit(self, cols, unit, comparison_only=False):
        """Returns a view that contains a new unit on one or more columns.

        The difference between :meth:`~getml.DataFrame.with_unit` and
        :meth:`~getml.DataFrame.set_unit` is that
        :meth:`~getml.DataFrame.with_unit` returns a view that is lazily
        evaluated when needed whereas :meth:`~getml.DataFrame.set_unit`
        is an in-place operation. From a memory perspective, in-place operations
        like :meth:`~getml.DataFrame.set_unit` are preferable.

        Args:
            cols (str, FloatColumn, StingColumn, or List[str, FloatColumn, StringColumn]):
                The columns or the names thereof.

            unit (str):
                The unit to be assigned.

            comparison_only (bool):
                Whether you want the column to
                be used for comparison only. This means that the column can
                only be used in comparison to other columns of the same unit.

                An example might be a bank account number: The number in itself
                is hardly interesting, but it might be useful to know how often
                we have seen that same bank account number in another table.

                If True, this will also set the
                :const:`~getml.data.subroles.only.compare` subrole.  The feature
                learning algorithms and the feature selectors will interpret this
                accordingly.
        """
        return _with_unit(self, cols, unit, comparison_only)


# --------------------------------------------------------------------


_all_attr = DataFrame("dummy").__dir__()


def _custom_dir(self):
    return _all_attr + self.colnames


DataFrame.__dir__ = _custom_dir  # type: ignore

# --------------------------------------------------------------------
