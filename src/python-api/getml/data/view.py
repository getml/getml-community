# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Contains the view."""

import json
import numbers
import os
from collections import namedtuple
from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd  # type: ignore

import getml.communication as comm
from getml.database import Connection
from getml.log import logger
from getml.utilities.formatting import _ViewFormatter

from .columns import BooleanColumnView, FloatColumn, FloatColumnView, arange, rowid
from .helpers import (
    _is_typed_list,
    _make_default_slice,
    _to_arrow,
    _to_parquet,
    _to_pyspark,
    _with_column,
    _with_role,
    _with_subroles,
    _with_unit,
)
from .placeholder import Placeholder
from .roles import (
    categorical,
    join_key,
    numerical,
    target,
    text,
    time_stamp,
    unused_float,
    unused_string,
)
from .roles_obj import Roles

# --------------------------------------------------------------------


class View:
    """A view is a lazily evaluated, immutable representation of a :class:`~getml.DataFrame`.

    There are important differences between a
    :class:`~getml.DataFrame` and a view:

        - Views are lazily evaluated. That means that views do not
          contain any data themselves. Instead, they just refer to
          an underlying data frame. If the underlying data frame changes,
          so will the view (but such behavior will result in a warning).

        - Views are immutable. In-place operations on a view are not
          possible. Any operation on a view will result in a new view.

        - Views have no direct representation on the getML engine and
          therefore they do not need to have an identifying name.

    Args:
        base (:class:`~getml.DataFrame` or :class:`~getml.data.View`):
            A data frame or view used as the basis for this view.

        name (str):
            The name assigned to this view.

        subselection (:class:`~getml.data.columns.BooleanColumnView`, :class:`~getml.data.columns.FloatColumnView` or :class:`~getml.data.columns.FloatColumn`):
            Indicates which rows we would like to keep.

        added (dict):
            A dictionary that describes a new column
            that has been added to the view.

        dropped (List[str]):
            A list of columns that have been dropped.

    Examples:
        You hardly ever directly create views. Instead, it is more likely
        that you will encounter them as a result of some operation on a
        :class:`~getml.DataFrame`:

        .. code-block:: python

            # Creates a view on the first 100 lines
            view1 = data_frame[:100]

            # Creates a view without some columns.
            view2 = data_frame.drop(["col1", "col2"])

            # Creates a view in which some roles are reassigned.
            view3 = data_frame.with_role(["col1", "col2"], getml.data.roles.categorical)

        A recommended pattern is to assign 'baseline roles' to your data frames
        and then using views to tweak them:

        .. code-block:: python

            # Assign baseline roles
            data_frame.set_role(["jk"], getml.data.roles.join_key)
            data_frame.set_role(["col1", "col2"], getml.data.roles.categorical)
            data_frame.set_role(["col3", "col4"], getml.data.roles.numerical)
            data_frame.set_role(["col5"], getml.data.roles.target)

            # Make the data frame immutable, so in-place operations are
            # no longer possible.
            data_frame.freeze()

            # Save the data frame.
            data_frame.save()

            # I suspect that col1 leads to overfitting, so I will drop it.
            view = data_frame.drop(["col1"])

            # Insert the view into a container.
            container = getml.data.Container(...)
            container.add(some_alias=view)
            container.save()

        The advantage of using such a pattern is that it enables you to
        always completely retrace your entire pipeline without creating
        deep copies of the data frames whenever you have made a small
        change like the one in our example. Note that the pipeline will
        record which :class:`~getml.data.Container` you have used.

    """

    # ------------------------------------------------------------

    def __init__(
        self,
        base,
        name: Optional[str] = None,
        subselection: Union[
            BooleanColumnView, FloatColumn, FloatColumnView, None
        ] = None,
        added=None,
        dropped: Optional[List[str]] = None,
    ):
        self._added = added
        self._base = deepcopy(base)
        self._dropped = dropped or []
        self._name = name
        self._subselection = subselection

        self._initial_timestamp: str = (
            self._base._initial_timestamp
            if isinstance(self._base, View)
            else self._base.last_change
        )

        self._base.refresh()

    # ------------------------------------------------------------

    def _apply_subselection(self, col):
        if self._subselection is not None:
            return col[self._subselection]
        return col

    # ------------------------------------------------------------

    def _collect_footer_data(self):
        footer = namedtuple("footer", ["n_rows", "n_cols", "type"])

        n_rows = "unknown number of" if self.nrows() == "unknown" else self.nrows()

        return footer(
            n_rows=n_rows,
            n_cols=len(self.colnames),
            type="getml.data.View",
        )

    # ------------------------------------------------------------

    def _format(self):
        return _ViewFormatter(self)

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

        if name in self.__dict__["_dropped"]:
            raise KeyError(
                "Cannot retrieve column '" + name + "'. It has been dropped."
            )

        if (
            self.__dict__["_added"] is not None
            and self.__dict__["_added"]["name_"] == name
        ):
            return self._apply_subselection(
                self.__dict__["_added"]["col_"]
                .with_subroles(self.__dict__["_added"]["subroles_"], append=False)
                .with_unit(self.__dict__["_added"]["unit_"])
            )

        return self._apply_subselection(self.__dict__["_base"][name])

    # ----------------------------------------------------------------

    def __len__(self):
        return self.nrows(force=True)

    # ------------------------------------------------------------

    def _getml_deserialize(self) -> Dict[str, Any]:
        cmd: Dict[str, Any] = {}

        cmd["type_"] = "View"

        if self._added is not None:
            added = deepcopy(self._added)
            col = deepcopy(added["col_"].cmd)
            col["last_change_"] = added["col_"].last_change
            added["col_"] = col
            cmd["added_"] = added

        if self._subselection is not None:
            subselection = deepcopy(self._subselection.cmd)
            subselection["last_change_"] = self.subselection.last_change
            cmd["subselection_"] = subselection

        cmd["base_"] = self._base._getml_deserialize()
        cmd["dropped_"] = self._dropped
        cmd["name_"] = self.name
        cmd["last_change_"] = self.last_change

        return cmd

    # ------------------------------------------------------------

    def _modify_colnames(self, base_names, role):
        remove_dropped = [name for name in base_names if name not in self._dropped]

        if self._added is not None and self._added["role_"] != role:
            return [name for name in remove_dropped if name != self._added["name_"]]

        if (
            self._added is not None
            and self._added["role_"] == role
            and self._added["name_"] not in remove_dropped
        ):
            return remove_dropped + [self._added["name_"]]

        return remove_dropped

    # ------------------------------------------------------------

    def __repr__(self):
        formatted = self._format()

        footer = self._collect_footer_data()

        return formatted._render_string(footer=footer)

    # ----------------------------------------------------------------

    def _repr_html_(self):
        formatted = self._format()

        footer = self._collect_footer_data()

        return formatted._render_html(footer=footer)

    # ------------------------------------------------------------

    @property
    def added(self):
        """
        The column that has been added to the view.
        """
        return deepcopy(self._added)

    # ------------------------------------------------------------

    @property
    def base(self):
        """
        The basis on which the view is created. Must be a
        :class:`~getml.DataFrame` or a :class:`~getml.data.View`.
        """
        return deepcopy(self._base)

    # ------------------------------------------------------------

    @property
    def _categorical_names(self):
        return self._modify_colnames(self._base._categorical_names, categorical)

    # ------------------------------------------------------------

    def check(self):
        """
        Checks whether the underlying data frame has been changed
        after the creation of the view.
        """
        last_change = self.last_change
        if last_change != self.__dict__["_initial_timestamp"]:
            logger.warning(
                "The data frame underlying view '"
                + self.name
                + "' was last changed at "
                + last_change
                + ", which was after the creation of the view. "
                + "This might lead to unexpected results. You might "
                + "want to recreate the view. (Views are lazily "
                + "evaluated, so recreating them is a very "
                + "inexpensive operation)."
            )

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
        Alias for :meth:`~getml.View.colnames`.

        Returns:
            List[str]:
                List of the names of all columns.
        """
        return self.colnames

    # ------------------------------------------------------------

    def drop(self, cols):
        """Returns a new :class:`~getml.data.View` that has one or several columns removed.

        Args:
            cols (str or List[str]):
                The names of the columns to be dropped.
        """
        if isinstance(cols, str):
            cols = [cols]

        if not _is_typed_list(cols, str):
            raise TypeError("'cols' must be a string or a list of strings.")

        return View(base=self, dropped=cols)

    # ------------------------------------------------------------

    @property
    def dropped(self):
        """
        The names of the columns that has been dropped.
        """
        return deepcopy(self._dropped)

    # ------------------------------------------------------------

    @property
    def last_change(self) -> str:
        """
        A string describing the last time this data frame has been changed.
        """
        return self.__dict__["_base"].last_change

    # ------------------------------------------------------------

    @property
    def _join_key_names(self):
        return self._modify_colnames(self._base._join_key_names, join_key)

    # ------------------------------------------------------------

    @property
    def name(self):
        """
        The name of the view. If no name is explicitly set,
        the name will be identical to the name of the base.
        """
        if self.__dict__["_name"] is None:
            return self.__dict__["_base"].name
        return deepcopy(self.__dict__["_name"])

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

    def nrows(self, force=False):
        """
        Returns the number of rows in the current instance.

        Args:
            force (bool, optional):
                If the number of rows is unknown,
                do you want to force the engine to calculate it anyway?
                This is a relatively expensive operation, therefore
                you might not necessarily want this.
        """

        self.refresh()

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "View.get_nrows"
        cmd["name_"] = ""

        cmd["cols_"] = [self[cname].cmd for cname in self.colnames]
        cmd["force_"] = force

        with comm.send_and_get_socket(cmd) as sock:
            json_str = comm.recv_string(sock)

        if json_str[0] != "{":
            comm.engine_exception_handler(json_str)

        result = json.loads(json_str)

        if "recordsTotal" in result:
            return int(result["recordsTotal"])

        return "unknown"

    # ------------------------------------------------------------

    @property
    def _numerical_names(self):
        return self._modify_colnames(self._base._numerical_names, numerical)

    # --------------------------------------------------------------------------

    def refresh(self):
        """Aligns meta-information of the current instance with the
        corresponding data frame in the getML engine.

        Returns:
            :class:`~getml.data.View`:
                Updated handle the underlying data frame in the getML
                engine.

        """
        self._base = self.__dict__["_base"].refresh()
        return self

    # ------------------------------------------------------------

    @property
    def roles(self):
        """
        The roles of the columns included
        in this View.
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

    @property
    def rowid(self):
        """
        The rowids for this view.
        """
        return rowid()[: len(self)]

    # ------------------------------------------------------------

    @property
    def subselection(self):
        """
        The subselection that is applied to this view.
        """
        return deepcopy(self._subselection)

    # ------------------------------------------------------------

    @property
    def _target_names(self):
        return self._modify_colnames(self._base._target_names, target)

    # ------------------------------------------------------------

    @property
    def _text_names(self):
        return self._modify_colnames(self._base._text_names, text)

    # ------------------------------------------------------------

    @property
    def _time_stamp_names(self):
        return self._modify_colnames(self._base._time_stamp_names, time_stamp)

    # ------------------------------------------------------------

    @property
    def shape(self):
        """
        A tuple containing the number of rows and columns of
        the View.
        """
        self.refresh()
        return (self.nrows(), self.ncols())

    # ------------------------------------------------------------

    def to_arrow(self):
        """Creates a :py:class:`pyarrow.Table` from the view.

        Loads the underlying data from the getML engine and constructs
        a :class:`pyarrow.Table`.

        Returns:
            :class:`pyarrow.Table`:
                Pyarrow equivalent of the current instance including
                its underlying data.
        """
        return _to_arrow(self)

    # ------------------------------------------------------------

    def to_json(self):
        """Creates a JSON string from the current instance.

        Loads the underlying data from the getML engine and constructs
        a JSON string.
        """
        return self.to_pandas().to_json()

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

        cmd["type_"] = "View.to_csv"
        cmd["name_"] = self.name

        cmd["view_"] = self._getml_deserialize()
        cmd["fname_"] = fname_
        cmd["quotechar_"] = quotechar
        cmd["sep_"] = sep
        cmd["batch_size_"] = batch_size

        comm.send(cmd)

    # ------------------------------------------------------------

    def to_db(self, table_name: str, conn: Optional[Connection] = None):
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

        conn = conn or Connection()

        self.refresh()

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be of type str")

        if not isinstance(conn, Connection):
            raise TypeError("'conn' must be a getml.database.Connection object or None")

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "View.to_db"
        cmd["name_"] = ""

        cmd["view_"] = self._getml_deserialize()
        cmd["table_name_"] = table_name
        cmd["conn_id_"] = conn.conn_id

        comm.send(cmd)

    # ------------------------------------------------------------

    def to_pandas(self):
        """Creates a :py:class:`pandas.DataFrame` from the view.

        Loads the underlying data from the getML engine and constructs
        a :class:`pandas.DataFrame`.

        Returns:
            :class:`pandas.DataFrame`:
                Pandas equivalent of the current instance including
                its underlying data.
        """
        return _to_arrow(self).to_pandas()

    # ------------------------------------------------------------

    def to_placeholder(self, name=None):
        """Generates a :class:`~getml.data.Placeholder` from the
        current :class:`~getml.data.View`.

        Args:
            name (str, optional):
                The name of the placeholder. If no
                name is passed, then the name of the placeholder will
                be identical to the name of the current view.

        Returns:
            :class:`~getml.data.Placeholder`:
                A placeholder with the same name as this data frame.


        """
        self.refresh()
        return Placeholder(name=name or self.name, roles=self.roles)

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

    def to_s3(
        self,
        bucket: str,
        key: str,
        region: str,
        sep: str = ",",
        batch_size: int = 50000,
    ):
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
            >>> your_view.to_s3(
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

        cmd["type_"] = "View.to_s3"
        cmd["name_"] = self.name

        cmd["view_"] = self._getml_deserialize()
        cmd["bucket_"] = bucket
        cmd["key_"] = key
        cmd["region_"] = region
        cmd["sep_"] = sep
        cmd["batch_size_"] = batch_size

        comm.send(cmd)

    # ------------------------------------------------------------

    @property
    def _unused_float_names(self):
        return self._modify_colnames(self._base._unused_float_names, unused_float)

    # ------------------------------------------------------------

    @property
    def _unused_names(self):
        return self._unused_float_names + self._unused_string_names

    # ------------------------------------------------------------

    @property
    def _unused_string_names(self):
        return self._modify_colnames(self._base._unused_string_names, unused_string)

    # ------------------------------------------------------------

    def where(self, index) -> "View":
        """Extract a subset of rows.

        Creates a new :class:`~getml.data.View` as a
        subselection of the current instance.

        Args:
            index (:class:`~getml.data.columns.BooleanColumnView` or :class:`~getml.data.columns.FloatColumnView` or :class:`~getml.data.columns.FloatColumn`):
                Boolean column indicating the rows you want to select.

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
            selector = arange(index, index + 1)
            return View(base=self, subselection=selector)

        if isinstance(index, slice):
            start, stop, step = _make_default_slice(index, len(self))
            selector = arange(start, stop, step)
            return View(base=self, subselection=selector)

        if isinstance(index, (BooleanColumnView, FloatColumn, FloatColumnView)):
            return View(base=self, subselection=index)

        raise TypeError("Unsupported type for a subselection: " + type(index).__name__)

    # ------------------------------------------------------------

    def with_column(
        self, col, name, role=None, unit="", subroles=None, time_formats=None
    ):
        """Returns a new :class:`~getml.data.View` that contains an additional column.

        Args:
            col (:mod:`~getml.column`):
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

    def with_role(self, names, role, time_formats=None):
        """Returns a new :class:`~getml.data.View` with modified roles.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The :code:`time_formats` argument is used to interpret :ref:`time
        format string <annotating_roles_time_stamp>`. For more information on
        roles, please refer to the :ref:`user guide <annotating>`.

        Args:
            names (str or List[str]):
                The name or names of the column.

            role (str):
                The role to be assigned.

            time_formats (str or List[str], optional):
                Formats to be used to parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.
        """
        return _with_role(self, names, role, time_formats)

    # ------------------------------------------------------------

    def with_subroles(self, names, subroles, append=True):
        """Returns a new view with one or several new subroles on one or more columns.

        Args:
            names (str or List[str]):
                The name or names of the column.

            subroles (str or List[str]):
                The subroles to be assigned.

            append (bool, optional):
                Whether you want to append the
                new subroles to the existing subroles.
        """
        return _with_subroles(self, names, subroles, append)

    # ------------------------------------------------------------

    def with_unit(self, names, unit, comparison_only=False):
        """Returns a view that contains a new unit on one or more columns.

        Args:
            names (str or List[str]):
                The name or names of the column.

            unit (str):
                The unit to be assigned.

            comparison_only (bool):
                Whether you want the column to
                be used for comparison only. This means that the column can
                only be used in comparison to other columns of the same unit.

                An example might be a bank account number: The number in itself
                is hardly interesting, but it might be useful to know how often
                we have seen that same bank account number in another table.

                If True, this will append ", comparison only" to the unit.
                The feature learning algorithms and the feature selectors will
                interpret this accordingly.
        """
        return _with_unit(self, names, unit, comparison_only)

    # ------------------------------------------------------------
