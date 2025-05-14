# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper functions that are not intended to be used by
the end-user.
"""

from __future__ import annotations

import json
import numbers
import os
import random
import string
from functools import lru_cache
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa

import getml.communication as comm
from getml import communication as comm
from getml import constants, data
from getml.data.columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    from_value,
)
from getml.data.columns.columns import arange
from getml.data.roles import sets as roles_sets
from getml.data.roles import (
    time_stamp,
    unused_float,
    unused_string,
)
from getml.data.roles.container import Roles
from getml.data.roles.types import Role
from getml.data.subroles import sets as subroles_sets
from getml.data.subroles.types import Subrole
from getml.database import Connection
from getml.database.helpers import _retrieve_temp_dir
from getml.helpers import _is_iterable_not_str_of_type

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame
    from getml.data.view import View

# --------------------------------------------------------------------

OnType = Optional[Union[str, Tuple[str, str], List[Union[str, Tuple[str, str]]]]]
"""
Types that can be passed to the 'on' argument of the 'join' method.
"""


# --------------------------------------------------------------------


def list_data_frames() -> Dict[str, List[str]]:
    """Lists all available data frames of the project.

    Returns:
        dict:
            Dict containing lists of strings representing the names of
            the data frames objects

            - 'in_memory'
                held in memory (RAM).
            - 'on_disk'
                stored on disk.

    ??? example
        ```python
        d, _ = getml.datasets.make_numerical()
        getml.data.list_data_frames()
        d.save()
        getml.data.list_data_frames()
        ```

    """

    cmd: Dict[str, Any] = {}
    cmd["type_"] = "list_data_frames"
    cmd["name_"] = ""

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            comm.handle_engine_exception(msg)
        json_str = comm.recv_string(sock)

    return json.loads(json_str)


# --------------------------------------------------------------------


def _check_if_exists(colnames: List[str], all_colnames: List[str]):
    for col in colnames:
        if col in all_colnames:
            raise ValueError("Duplicate column: '" + col + "'!")

        all_colnames.append(col)

    return all_colnames


# --------------------------------------------------------------------


def _check_join_key(candidates: Any, roles: Roles, name: str):
    if isinstance(candidates, str):
        candidates = [candidates]

    not_existing = [
        candidate
        for candidate in candidates
        if candidate is not None and candidate not in roles.columns
    ]

    if not_existing:
        raise ValueError(
            f"Columns {not_existing!r} not found in schema of Placeholder {name!r}."
        )

    invalid = [
        candidate
        for candidate in candidates
        if candidate is not None and candidate not in roles.join_key
    ]

    if invalid:
        raise ValueError(
            f"Columns {invalid!r} don't have the role 'join_key' "
            "In the schema of Placeholder {name!r}."
        )


# --------------------------------------------------------------------


def _empty_data_frame() -> str:
    return "Empty getml.DataFrame\nColumns: []\n"


# --------------------------------------------------------------------


def _exists_in_memory(name: str) -> bool:
    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    all_df = list_data_frames()

    return name in all_df["in_memory"]


# --------------------------------------------------------------------


def _finditems(key: str, dct: Dict[str, Any]) -> Any:
    if key in dct:
        yield dct[key]
    for k, v in dct.items():
        if isinstance(v, dict):
            yield from _finditems(key, v)


# --------------------------------------------------------------------


def _get_column(name, columns):
    for col in columns:
        if col.name == name:
            return col
    return None


# --------------------------------------------------------------------


def _handle_cols(
    cols: Union[
        str,
        FloatColumn,
        StringColumn,
        Union[Iterable[str], Iterable[FloatColumn], Iterable[StringColumn]],
    ],
) -> List[str]:
    """
    Handles cols as supplied to DataFrame methods. Returns a list of column names.
    """

    if _is_iterable_not_str_of_type(cols, str):
        cols_ = cols
    elif isinstance(col := cols, str):
        cols_ = [col]
    else:
        raise TypeError(
            "'cols' must be either a string, a FloatColumn, "
            "a StringColumn, or a list thereof."
        )

    names: List[str] = []

    for col in cols_:
        if isinstance(col, list):
            names.extend(_handle_cols(col))

        if isinstance(col, str):
            names.append(col)

        if isinstance(col, (FloatColumn, StringColumn)):
            names.append(col.name)

    return names


# --------------------------------------------------------------------


def _handle_multiple_join_keys(
    join_keys: List[Union[str, Tuple[str, str]]],
) -> List[Tuple[str, str]]:
    return [(jk, jk) if isinstance(jk, str) else jk for jk in join_keys]


# --------------------------------------------------------------------


def _handle_on(on: OnType) -> Union[List[Tuple[str, str]], List[Tuple[None, None]]]:
    if isinstance(on, str):
        return [(on, on)]

    if isinstance(on, list):
        return _handle_multiple_join_keys(on)

    if isinstance(on, tuple):
        return [on]

    if on is None:
        return [(None, None)]


# --------------------------------------------------------------------


def _handle_ts(ts):
    if ts is None:
        return ("", "")
    return ts


# --------------------------------------------------------------------


def _is_numerical_type_numpy(coltype) -> bool:
    return coltype in [
        int,
        float,
        np.int_,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
        np.float_,
        np.float16,
        np.float32,
        np.float64,
    ]


# --------------------------------------------------------------------


def _is_subclass_list(some_list, parent) -> bool:
    is_subclass_list = isinstance(some_list, list)

    is_subclass_list = is_subclass_list and all(
        [issubclass(type(ll), parent) for ll in some_list]
    )

    return is_subclass_list


# --------------------------------------------------------------------


def _is_typed_dict(some_dict, key_types, value_types) -> bool:
    if not isinstance(key_types, list):
        key_types = [key_types]

    if not isinstance(value_types, list):
        value_types = [value_types]

    is_typed_dict = isinstance(some_dict, dict)

    is_typed_dict = is_typed_dict and all(
        [any([isinstance(key, t) for t in key_types]) for key in some_dict.keys()]
    )

    is_typed_dict = is_typed_dict and all(
        [any([isinstance(val, t) for t in value_types]) for val in some_dict.values()]
    )

    return is_typed_dict


# --------------------------------------------------------------------


def _is_typed_list(some_list, types) -> bool:
    if isinstance(types, list):
        types = tuple(types)

    is_typed_list = isinstance(some_list, list)

    is_typed_list = is_typed_list and all([isinstance(ll, types) for ll in some_list])

    return is_typed_list


# --------------------------------------------------------------------


def _is_non_empty_typed_list(some_list, types) -> bool:
    return _is_typed_list(some_list, types) and len(some_list) > 0


# -----------------------------------------------------------------


def _iter_batches(
    df_or_view: Union[DataFrame, View], batch_size: int
) -> Iterator[View]:
    start, end = 0, batch_size
    while batch := df_or_view[start:end]:
        yield batch
        start, end = end, end + batch_size


# -----------------------------------------------------------------


def _make_id() -> str:
    letters = string.ascii_letters + string.digits
    return "".join([random.choice(letters) for _ in range(6)])


# --------------------------------------------------------------------


def _infer_arange_args_from_slice(
    slc: slice, df_or_view: Union[DataFrame, View]
) -> Tuple[int, int, int]:
    @lru_cache(maxsize=None)
    def len_df_or_view():
        return len(df_or_view)

    if slc.step == 0:
        raise ValueError("slice step cannot be zero")

    start = slc.start
    stop = slc.stop
    step = slc.step or 1

    # as len forces an evaluation of the view, we want to call
    # it only if necessary and only once

    if start is None:
        if step > 0:
            start = 0
        elif step < 0:
            start = len_df_or_view() - 1
    elif start < 0:
        start = max(len_df_or_view() + start, 0)

    if stop is None:
        if step > 0:
            stop = len_df_or_view()
        elif step < 0:
            stop = -1
    elif stop < 0:
        stop = max(len_df_or_view() + stop, 0)

    if (stop < start and step > 0) or (stop > start and step < 0):
        return 0, 0, 1

    return start, stop, step


# --------------------------------------------------------------------


def _serialize_join_keys(on: OnType) -> Tuple[str, str]:
    if on == [(None, None)]:
        return constants.NO_JOIN_KEY, constants.NO_JOIN_KEY

    join_key, other_join_key = zip(*on)
    begin = constants.MULTIPLE_JOIN_KEYS_BEGIN
    end = constants.MULTIPLE_JOIN_KEYS_END
    sep = constants.JOIN_KEY_SEP
    len_jk = len_other_jk = 1

    if not other_join_key:
        other_join_key = join_key

    len_jk = len(join_key)
    if len_jk > 1:
        join_key_merged = begin + sep.join(join_key) + end
    else:
        join_key_merged = join_key[0]

    len_other_jk = len(other_join_key)
    if len_other_jk > 1:
        other_join_key_merged = begin + sep.join(other_join_key) + end
    else:
        other_join_key_merged = other_join_key[0]

    if len_jk != len_other_jk:
        raise ValueError(
            "The number of join keys passed to "
            + "'join_key' and 'other_join_key' "
            + "must match!"
        )

    return join_key_merged, other_join_key_merged


# --------------------------------------------------------------------


def _prepare_roles(
    roles: Optional[Union[Roles, Dict[Role, Iterable[str]]]],
    sniffed_roles: Roles,
    ignore_sniffed_roles: bool = False,
) -> Roles:
    if roles is None:
        roles = {}

    if isinstance(roles, dict):
        roles = Roles.from_dict(roles)

    if ignore_sniffed_roles:
        return roles

    roles = sniffed_roles.update(roles)

    return roles


# --------------------------------------------------------------------


# --------------------------------------------------------------------


def _remove_trailing_underscores(some_dict: Dict[str, Any]) -> Dict[str, Any]:
    new_dict: Dict[str, Any] = {}

    for kkey in some_dict:
        new_key = kkey

        if kkey[-1] == "_":
            new_key = kkey[:-1]

        if isinstance(some_dict[kkey], dict):
            new_dict[new_key] = _remove_trailing_underscores(some_dict[kkey])

        elif isinstance(some_dict[kkey], list):
            new_dict[new_key] = [
                _remove_trailing_underscores(elem) if isinstance(elem, dict) else elem
                for elem in some_dict[kkey]
            ]

        else:
            new_dict[new_key] = some_dict[kkey]

    return new_dict


# --------------------------------------------------------------------


def _replace_non_alphanumeric(string: str) -> str:
    replaced = "".join([" " if not c.isalnum() else c for c in list(string)])
    stripped = replaced.strip()
    result = stripped.replace(" ", "_")
    while "___" in result:
        result.replace("___", "__")
    return result


# --------------------------------------------------------------------


def _replace_non_alphanumeric_cols(df_or_view):
    """
    Many databases only accept alphanumeric characters
    and underscores. This is meant to handle these
    problems.
    """
    non_alnum_colnames = [
        cname
        for cname in df_or_view.colnames
        if not cname.isalnum() and _replace_non_alphanumeric(cname) != cname
    ]

    if not non_alnum_colnames:
        return df_or_view

    for old_name in non_alnum_colnames:
        col = df_or_view[old_name]
        new_name = _replace_non_alphanumeric(old_name)
        if new_name != old_name:
            df_or_view = df_or_view.with_column(
                col=col,
                name=new_name,
                role=df_or_view.roles.column(old_name),
                subroles=col.subroles,
                unit=col.unit,
                time_formats=None,
            )

    return df_or_view.drop(non_alnum_colnames)


# --------------------------------------------------------------------


def _make_table(col, numpy_array) -> pa.Table:
    data_frame = pd.DataFrame()
    data_frame[col.name] = numpy_array
    return pa.Table.from_pandas(data_frame)


# ------------------------------------------------------------


def _send_numpy_array(col, numpy_array: np.ndarray):
    table = _make_table(col, numpy_array)

    with comm.send_and_get_socket(col.cmd) as sock:
        with sock.makefile(mode="wb") as sink:
            batches = table.to_batches()
            with pa.ipc.new_stream(sink, table.schema) as writer:
                for batch in batches:
                    writer.write_batch(batch)

        msg = comm.recv_string(sock)

    if msg != "Success!":
        comm.handle_engine_exception(msg)


# --------------------------------------------------------------------


def _sniff_db(table_name: str, conn: Optional[Connection] = None) -> Roles:
    """
    Sniffs a table in the database and returns a dictionary of roles.

    Args:
        table_name: Name of the table to be sniffed.

        conn: The database
            connection to be used. If you don't explicitly pass a connection,
            the Engine will use the default connection.

    Returns:
        Keyword arguments (kwargs) that can be used to construct a DataFrame.
    """

    conn = conn or Connection()

    cmd: Dict[str, Any] = {}

    cmd["name_"] = table_name
    cmd["type_"] = "Database.sniff_table"

    cmd["conn_id_"] = conn.conn_id

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            raise Exception(msg)
        roles = comm.recv_string(sock)

    return Roles.from_dict(json.loads(roles))


# --------------------------------------------------------------------


def _sniff_query(query: str, name: str, conn: Optional[Connection] = None) -> Roles:
    """
    Sniffs a table in the database and returns a dictionary of roles.

    Args:
        name: Name of the resulting DataFrame.

        conn: The database connection to be used. If you don't explicitly pass a
            connection, the engine will use the default connection.
    """

    conn = conn or Connection()

    cmd: Dict[str, str] = {}

    cmd["name_"] = name
    cmd["type_"] = "Database.sniff_query"

    cmd["conn_id_"] = conn.conn_id

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.send_string(sock, query)
        msg = comm.recv_string(sock)
        if msg != "Success!":
            sock.close()
            raise Exception(msg)
        roles = comm.recv_string(sock)

    return Roles.from_dict(json.loads(roles))


# --------------------------------------------------------------------


def _sniff_s3(
    bucket: str,
    keys: Iterable[str],
    region: str,
    num_lines_sniffed: Optional[int] = 1000,
    sep: Optional[str] = ",",
    skip: Optional[int] = 0,
    colnames: Optional[Iterable[str]] = None,
) -> Roles:
    """Sniffs a list of CSV files located in an S3 bucket
    and returns the result as a dictionary of roles.

    Args:
        bucket:
            The bucket from which to read the files.

        keys: The list of keys (files in the bucket) to be read.

        region:
            The region in which the bucket is located.

        num_lines_sniffed:
            Number of lines analysed by the sniffer.

        sep:
            The character used for separating fields.

        skip:
            Number of lines to skip at the beginning of each file.

        colnames: The first line of a CSV file
            usually contains the column names. When this is not the case, you need to
            explicitly pass them.

    Returns:
        Keyword arguments (kwargs) that can be used to construct a DataFrame.
    """

    cmd: Dict[str, Any] = {}

    cmd["name_"] = ""
    cmd["type_"] = "Database.sniff_s3"

    cmd["bucket_"] = bucket
    cmd["dialect_"] = "python"
    cmd["keys_"] = keys
    cmd["num_lines_sniffed_"] = num_lines_sniffed
    cmd["region_"] = region
    cmd["sep_"] = sep
    cmd["skip_"] = skip
    cmd["conn_id_"] = "default"

    if colnames is not None:
        cmd["colnames_"] = colnames

    with comm.send_and_get_socket(cmd) as sock:
        msg = comm.recv_string(sock)
        if msg != "Success!":
            raise OSError(msg)
        roles = comm.recv_string(sock)

    return Roles.from_dict(json.loads(roles))


# ------------------------------------------------------------


def _to_pyspark(df_or_view: Any, name: str, spark: Any):
    if df_or_view.ncols() == 0:
        raise ValueError(
            "Cannot transform '"
            + df_or_view.name
            + "' to pyspark.DataFrame, because it contains no columns."
        )
    temp_dir = _retrieve_temp_dir()
    path = (temp_dir / (name + ".parquet")).as_posix()

    _replace_non_alphanumeric_cols(df_or_view).to_parquet(path, coerce_timestamps="ms")
    spark_df = spark.read.parquet(path)
    spark_df.createOrReplaceTempView(name)
    spark.sql("CACHE TABLE `" + name + "`;")
    os.remove(path)
    return spark_df


# ------------------------------------------------------------


def _transform_col(col, role, is_boolean, is_float, is_string, time_formats):
    if (is_boolean or is_float) and role in roles_sets.categorical:
        return col.as_str()  # pytype: disable=attribute-error

    if (is_boolean or is_string) and role in roles_sets.numerical:
        if role == time_stamp:
            return col.as_ts(  # pytype: disable=attribute-error
                time_formats=time_formats
            )

        return col.as_num()  # pytype: disable=attribute-error

    return col


# --------------------------------------------------------------------


def _where(
    df_or_view: Union[DataFrame, View],
    indeces_or_mask: Union[
        numbers.Real, slice, BooleanColumnView, FloatColumnView, FloatColumn
    ],
):
    if isinstance(indeces_or_mask, numbers.Real):
        index = int(indeces_or_mask)
        index = index if index >= 0 else len(df_or_view) + index
        selection = arange(index, index + 1)
        candidate_view = data.View(base=df_or_view, subselection=selection)

        # in python, a slice's stop can exceed the length of the container,
        # so we can carry out the bounds check on a minimum subselection
        # (1 row) to avoid evaluation of the full view
        if len(candidate_view[:1]) == 0:
            raise IndexError("Index out of bounds.")

        return candidate_view

    if isinstance(slice_ := indeces_or_mask, slice):
        start, stop, step = _infer_arange_args_from_slice(slice_, df_or_view)
        selection = arange(start, stop, step)
        return data.View(base=df_or_view, subselection=selection)

    if isinstance(
        mask := indeces_or_mask, (BooleanColumnView, FloatColumn, FloatColumnView)
    ):
        return data.View(base=df_or_view, subselection=mask)

    raise TypeError(
        "Unsupported type for a subselection: " + type(indeces_or_mask).__name__
    )


# --------------------------------------------------------------------


def _with_column(
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
    # ------------------------------------------------------------

    if isinstance(col, (bool, str, int, float, numbers.Number, np.datetime64)):
        col = from_value(col)

    # ------------------------------------------------------------

    if not isinstance(
        col,
        (
            FloatColumn,
            FloatColumnView,
            StringColumn,
            StringColumnView,
            BooleanColumnView,
        ),
    ):
        raise TypeError("'col' must be a getml.data.Column or a value!")

    # ------------------------------------------------------------

    is_boolean = isinstance(col, BooleanColumnView)

    if not is_boolean:
        subroles = subroles or col.subroles  # type: ignore
        unit = unit or col.unit  # type: ignore

    # ------------------------------------------------------------

    subroles = subroles or []

    if isinstance(subroles, str):
        subroles = [subroles]

    # ------------------------------------------------------------

    if not isinstance(name, str):
        raise TypeError("'name' must be of type str")

    if not _is_typed_list(subroles, str):
        raise TypeError("'subroles' must be of type str, List[str] or None.")

    if not isinstance(unit, str):
        raise TypeError("'unit' must be of type str")

    # ------------------------------------------------------------

    invalid_subroles = [r for r in subroles if r not in subroles_sets.all_]

    if invalid_subroles:
        raise ValueError(
            "'subroles' must be from getml.data.subroles, "
            + "meaning it is one of the following: "
            + str(subroles_sets.all_)
            + ", got "
            + str(invalid_subroles)
        )

    # ------------------------------------------------------------

    time_formats = time_formats or constants.TIME_FORMATS

    # ------------------------------------------------------------

    is_string = "StringColumn" in col.cmd["type_"]

    is_float = "FloatColumn" in col.cmd["type_"]

    # ------------------------------------------------------------

    correct_coltype = is_boolean or is_string or is_float

    if not correct_coltype:
        raise TypeError("""'col' must be a getml.data.Column.""")

    # ------------------------------------------------------------

    if role is None:
        role = unused_float if is_float else unused_string

    Roles.from_dict({role: [name]}).validate()

    # ------------------------------------------------------------

    return (
        _transform_col(col, role, is_boolean, is_float, is_string, time_formats),
        role,
        subroles,
    )


# --------------------------------------------------------------------


def _with_role(
    base,
    cols: Union[
        str,
        FloatColumn,
        StringColumn,
        Union[List[str], List[FloatColumn], List[StringColumn]],
    ],
    role: str,
    time_formats: Optional[List[str]],
):
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
        if nname not in base.colnames:
            raise ValueError("No column called '" + nname + "' found.")

    if role not in roles_sets.all_:
        raise ValueError(
            "'role' must be one of the following values: " + str(roles_sets.all_)
        )

    # ------------------------------------------------------------

    view = base

    for name in names:
        col = view[name]
        unit = (
            constants.TIME_STAMP + constants.COMPARISON_ONLY
            if role == time_stamp
            else col.unit
        )
        view = view.with_column(
            col=col,
            name=name,
            role=role,
            subroles=col.subroles,
            unit=unit,
            time_formats=time_formats,
        )

    # ------------------------------------------------------------

    return view


# ------------------------------------------------------------


def _with_subroles(
    base,
    cols: Union[
        str,
        FloatColumn,
        StringColumn,
        Union[List[str], List[FloatColumn], List[StringColumn]],
    ],
    subroles: Union[Subrole, Iterable[str]],
    append: bool = False,
):
    names = _handle_cols(cols)

    if isinstance(subroles, str):
        subroles = [subroles]

    if not _is_non_empty_typed_list(names, str):
        raise TypeError("'names' must be either a string or a list of strings.")

    if not _is_typed_list(subroles, str):
        raise TypeError("'subroles' must be either a string or a list of strings.")

    if not isinstance(append, bool):
        raise TypeError("'append' must be a bool.")

    # ------------------------------------------------------------

    if [r for r in subroles if r not in subroles_sets.all_]:
        raise ValueError(
            "'subroles' must be from getml.data.subroles, "
            + "meaning it is one of the following: "
            + str(subroles_sets.all_)
        )

    # ------------------------------------------------------------

    view = base

    for name in names:
        col = view[name]
        view = view.with_column(
            col=col,
            name=name,
            role=view.roles.column(name),
            subroles=subroles,
            unit=col.unit,
            time_formats=None,
        )

    # ------------------------------------------------------------

    return view


# ------------------------------------------------------------


def _with_unit(
    base,
    cols: Union[
        str,
        FloatColumn,
        StringColumn,
        Union[List[str], List[FloatColumn], List[StringColumn]],
    ],
    unit: str,
    comparison_only=False,
):
    names = _handle_cols(cols)

    if not isinstance(unit, str):
        raise TypeError("Parameter 'unit' must be a str.")

    # ------------------------------------------------------------

    if comparison_only:
        unit += constants.COMPARISON_ONLY

    # ------------------------------------------------------------

    view = base

    for name in names:
        col = view[name]
        view = view.with_column(
            col=col,
            name=name,
            role=view.roles.column(name),
            subroles=col.subroles,
            unit=unit,
            time_formats=None,
        )

    # ------------------------------------------------------------

    return view


# --------------------------------------------------------------------
