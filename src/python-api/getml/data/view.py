# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Contains the view."""

from __future__ import annotations

import json
import numbers
import warnings
from collections import namedtuple
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
    cast,
)

import numpy as np
import pandas as pd
import pyarrow as pa

import getml.communication as comm
from getml import data
from getml.constants import DEFAULT_BATCH_SIZE
from getml.data._io.arrow import to_arrow, to_arrow_stream
from getml.data._io.csv import to_csv
from getml.data._io.parquet import to_parquet
from getml.data.columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    rowid,
)
from getml.data.helpers import (
    _is_typed_list,
    _iter_batches,
    _to_pyspark,
    _where,
    _with_column,
    _with_role,
    _with_subroles,
    _with_unit,
)
from getml.data.placeholder import Placeholder
from getml.data.roles import (
    categorical,
    join_key,
    numerical,
    target,
    text,
    time_stamp,
    unused_float,
    unused_string,
)
from getml.data.roles.container import Roles
from getml.data.roles.types import Role
from getml.data.subroles.types import Subrole
from getml.database import Connection
from getml.log import logger
from getml.utilities.formatting import _ViewFormatter

if TYPE_CHECKING:
    import pyspark.sql

    from getml.data import DataFrame

# --------------------------------------------------------------------


class View:
    """A view is a lazily evaluated, immutable representation of a [`DataFrame`][getml.DataFrame].

    There are important differences between a
    [`DataFrame`][getml.DataFrame] and a view:

    - Views are lazily evaluated. That means that views do not
      contain any data themselves. Instead, they just refer to
      an underlying data frame. If the underlying data frame changes,
      so will the view (but such behavior will result in a warning).

    - Views are immutable. In-place operations on a view are not
      possible. Any operation on a view will result in a new view.

    - Views have no direct representation on the getML Engine, and
      therefore they do not need to have an identifying name.

    Attributes:
        base:
            A data frame or view used as the basis for this view.

        name:
            The name assigned to this view.

        subselection:
            Indicates which rows we would like to keep.

        added:
            A dictionary that describes a new column
            that has been added to the view.

        dropped:
            A list of columns that have been dropped.

    ??? example
        You hardly ever directly create views. Instead, it is more likely
        that you will encounter them as a result of some operation on a
        [`DataFrame`][getml.DataFrame]:

        ```python

        # Creates a view on the first 100 lines
        view1 = data_frame[:100]

        # Creates a view without some columns.
        view2 = data_frame.drop(["col1", "col2"])

        # Creates a view in which some roles are reassigned.
        view3 = data_frame.with_role(["col1", "col2"], getml.data.roles.categorical)
        ```
        A recommended pattern is to assign 'baseline roles' to your data frames
        and then using views to tweak them:

        ```python
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
        ```

        The advantage of using such a pattern is that it enables you to
        always completely retrace your entire pipeline without creating
        deep copies of the data frames whenever you have made a small
        change like the one in our example. Note that the pipeline will
        record which [`Container`][getml.data.Container] you have used.

    """

    # ------------------------------------------------------------

    def __init__(
        self,
        base: Union[DataFrame, View],
        name: Optional[str] = None,
        subselection: Optional[
            Union[BooleanColumnView, FloatColumn, FloatColumnView]
        ] = None,
        added: Optional[Dict] = None,
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

    def __len__(self) -> int:
        return cast(int, self.nrows(force=True))

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
    def added(self) -> Dict:
        """
        The column that has been added to the view.

        Returns:
                The column that has been added to the view.
        """
        return deepcopy(self._added)

    # ------------------------------------------------------------

    @property
    def base(self) -> Union[DataFrame, View]:
        """
        The basis on which the view is created. Must be a
        [`DataFrame`][getml.DataFrame] or a [`View`][getml.data.View].

        Returns:
                The basis on which the view is created.
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
        Alias for [`colnames`][getml.data.View.colnames].

        Returns:
                List of the names of all columns.
        """
        return self.colnames

    # ------------------------------------------------------------

    def drop(self, cols: Union[str, List[str]]) -> View:
        """Returns a new [`View`][getml.data.View] that has one or several columns removed.

        Args:
            cols:
                The names of the columns to be dropped.

        Returns:
                A new view with the specified columns removed.
        """
        if isinstance(cols, str):
            cols = [cols]

        if not _is_typed_list(cols, str):
            raise TypeError("'cols' must be a string or a list of strings.")

        return View(base=self, dropped=cols)

    # ------------------------------------------------------------

    @property
    def dropped(self) -> List[str]:
        """
        The names of the columns that has been dropped.

        Returns:
                The names of the columns that has been dropped.
        """
        return deepcopy(self._dropped)

    # ------------------------------------------------------------

    def iter_batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> Iterator[View]:
        yield from _iter_batches(self, batch_size)

    # ------------------------------------------------------------

    @property
    def last_change(self) -> str:
        """
        A string describing the last time this data frame has been changed.

        Returns:
                A string describing the last time this data frame has been changed.
        """
        return self.__dict__["_base"].last_change

    # ------------------------------------------------------------

    @property
    def _join_key_names(self):
        return self._modify_colnames(self._base._join_key_names, join_key)

    # ------------------------------------------------------------

    @property
    def name(self) -> str:
        """
        The name of the view. If no name is explicitly set,
        the name will be identical to the name of the base.

        Returns:
                The name of the view.

        """
        if self.__dict__["_name"] is None:
            return self.__dict__["_base"].name
        return deepcopy(self.__dict__["_name"])

    # ------------------------------------------------------------

    def ncols(self) -> int:
        """
        Number of columns in the current instance.

        Returns:
                Overall number of columns
        """
        return len(self.colnames)

    # ------------------------------------------------------------

    def nrows(self, force: bool = False) -> Union[int, str]:
        """
        Returns the number of rows in the current instance.

        Args:
            force:
                If the number of rows is unknown,
                do you want to force the Engine to calculate it anyway?
                This is a relatively expensive operation, therefore
                you might not necessarily want this.

        Returns:
                The number of rows in the current instance.
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
            comm.handle_engine_exception(json_str)

        response = json.loads(json_str)

        if "recordsTotal" in response:
            return response["recordsTotal"]

        # ensure that we do not display "unknown" if the number of rows is
        # less than or equal to the maximum number of diplayed rows
        nrows_to_display = len(self[: _ViewFormatter.max_rows + 1])
        if nrows_to_display <= _ViewFormatter.max_rows:
            return nrows_to_display

        return "unknown"

    # ------------------------------------------------------------

    @property
    def _numerical_names(self):
        return self._modify_colnames(self._base._numerical_names, numerical)

    # --------------------------------------------------------------------------

    def refresh(self) -> View:
        """Aligns meta-information of the current instance with the
        corresponding data frame in the getML Engine.

        Returns:
                Updated handle the underlying data frame in the getML
                Engine.

        """
        self._base = self.__dict__["_base"].refresh()
        return self

    # ------------------------------------------------------------

    @property
    def roles(self) -> Roles:
        """
        The roles of the columns included
        in this View.

        Returns:
                The roles of the columns included in this View.
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

    @property
    def rowid(self) -> List[int]:
        """
        The rowids for this view.

        Returns:
                The rowids for this view.
        """
        return rowid()[: len(self)]

    # ------------------------------------------------------------

    @property
    def subselection(self) -> Union[BooleanColumnView, FloatColumn, FloatColumnView]:
        """
        The subselection that is applied to this view.

        Returns:
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
    def shape(self) -> Tuple[Union[int, str], int]:
        """
        A tuple containing the number of rows and columns of
        the View.
        """
        self.refresh()
        return self.nrows(), self.ncols()

    # ------------------------------------------------------------
    def to_arrow(self) -> pa.Table:
        """Creates a `pyarrow.Table` from the view.

        Loads the underlying data from the getML Engine and constructs
        a `pyarrow.Table`.

        Returns:
                Pyarrow equivalent of the current instance including
                its underlying data.
        """
        return to_arrow(self)

    # ------------------------------------------------------------

    @contextmanager
    def to_arrow_stream(self) -> Iterator[pa.RecordBatchReader]:
        """
        Streams the view as an Apache Arrow `pa.RecordBatchReader`.

        This method provides a way to access the view as an Apache Arrow
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
            >>> generated_view = generated[:100]

            >>> con = duckdb.connect()

            >>> # Use the context manager to get the Arrow stream
            >>> with generated_view.to_arrow_stream() as arrow_stream_reader:
            ...     # Register the Arrow stream as a duckdb relation
            ...     con.register("generated", arrow_stream_reader)
            ...
            ...     # Now you can query the data using SQL
            ...     count = con.execute("SELECT COUNT(*) FROM generated").df()
            ...     print(count)
            # count_star()
            # 100
        """
        with to_arrow_stream(self) as stream:
            yield stream

    # ------------------------------------------------------------

    def to_json(self) -> str:
        """Creates a JSON string from the current instance.

        Loads the underlying data from the getML Engine and constructs
        a JSON string.

        Returns:
                JSON string of the current instance including its
                underlying data.
        """
        return self.to_pandas().to_json()

    def to_csv(
        self,
        fname: str,
        quotechar: str = '"',
        sep: str = ",",
        batch_size: int = 0,
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

    def to_df(self, name) -> DataFrame:
        """Creates a [`DataFrame`][getml.DataFrame] from the view."""
        self.check()
        self = self.refresh()
        df = data.DataFrame(name)
        return df.read_view(self)

    # ------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """Creates a `pandas.DataFrame` from the view.

        Loads the underlying data from the getML Engine and constructs
        a `pandas.DataFrame`.

        Returns:
                Pandas equivalent of the current instance including
                its underlying data.
        """
        return to_arrow(self).to_pandas()

    # ------------------------------------------------------------

    def to_placeholder(self, name: Optional[str] = None) -> Placeholder:
        """Generates a [`Placeholder`][getml.data.Placeholder] from the
        current [`View`][getml.data.View].

        Args:
            name:
                The name of the placeholder. If no
                name is passed, then the name of the placeholder will
                be identical to the name of the current view.

        Returns:
                A placeholder with the same name as this data frame.


        """
        self.refresh()
        return Placeholder(name=name or self.name, roles=self.roles)

    # ------------------------------------------------------------

    def to_parquet(
        self,
        fname: str,
        compression: Literal["brotli", "gzip", "lz4", "snappy", "zstd"] = "snappy",
        coerce_timestamps: Optional[bool] = None,
    ):
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
                If none is passed, then the name of this
                [`DataFrame`][getml.DataFrame] will be used.

        Returns:
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

        Note:
            S3 is not supported on Windows.

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

            your_view.to_s3(
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

    def where(
        self, index: Optional[Union[BooleanColumnView, FloatColumn, FloatColumnView]]
    ) -> View:
        """Extract a subset of rows.

        Creates a new [`View`][getml.data.View] as a
        subselection of the current instance.

        Args:
            index:
                Boolean column indicating the rows you want to select.

        Returns:
            A new view containing only the rows that satisfy the condition.

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
        unit: str = "",
        subroles: Optional[Union[Subrole, Iterable[str]]] = None,
        time_formats: Optional[List[str]] = None,
    ) -> View:
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

        Returns:
            A new view containing the additional column.
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
            name (str):
                The name of the new view.

        Returns:
            A new view with the new name.
        """
        return View(base=self, name=name)

    # ------------------------------------------------------------

    def with_role(
        self,
        names: Union[str, List[str]],
        role: str,
        time_formats: Optional[List[str]] = None,
    ) -> View:
        """Returns a new [`View`][getml.data.View] with modified roles.

        When switching from a role based on type float to a role based on type
        string or vice verse, an implicit type conversion will be conducted.
        The `time_formats` argument is used to interpret time
        format string: `annotating_roles_time_stamp`. For more information on
        roles, please refer to the [User Guide][annotating-data].

        Args:
            names:
                The name or names of the column.

            role:
                The role to be assigned.

            time_formats:
                Formats to be used to parse the time stamps.
                This is only necessary, if an implicit conversion from a StringColumn to
                a time stamp is taking place.

        Returns:
            A new view with the modified roles.
        """
        return _with_role(self, names, role, time_formats)

    # ------------------------------------------------------------

    def with_subroles(
        self,
        names: Union[str, List[str]],
        subroles: Union[Subrole, Iterable[str]],
        append: bool = True,
    ) -> View:
        """Returns a new view with one or several new subroles on one or more columns.

        Args:
            names:
                The name or names of the column.

            subroles:
                The subroles to be assigned.

            append:
                Whether you want to append the
                new subroles to the existing subroles.

        Returns:
            A new view with the modified subroles.
        """
        return _with_subroles(self, names, subroles, append)

    # ------------------------------------------------------------

    def with_unit(
        self, names: Union[str, List[str]], unit: str, comparison_only: bool = False
    ) -> View:
        """Returns a view that contains a new unit on one or more columns.

        Args:
            names:
                The name or names of the column.

            unit:
                The unit to be assigned.

            comparison_only:
                Whether you want the column to
                be used for comparison only. This means that the column can
                only be used in comparison to other columns of the same unit.

                An example might be a bank account number: The number in itself
                is hardly interesting, but it might be useful to know how often
                we have seen that same bank account number in another table.

                If True, this will append ", comparison only" to the unit.
                The feature learning algorithms and the feature selectors will
                interpret this accordingly.

        Returns:
            A new view with the modified unit.
        """
        return _with_unit(self, names, unit, comparison_only)

    # ------------------------------------------------------------
