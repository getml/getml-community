# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains simple helper functions for the sqlite3 module
"""

import importlib
import logging
from inspect import cleandoc
from typing import Optional

import numpy as np

# ----------------------------------------------------------------------------


def _try_import_scipy():
    try:
        return importlib.import_module("scipy")
    except ImportError:
        raise ImportError(
            "The 'scipy' package is required for custom sqlite3 aggregations."
        )


# ----------------------------------------------------------------------------


def _create_table(conn, table_name, schema, if_exists="append"):
    if _table_exists(conn, table_name):
        if if_exists == "fail":
            raise ValueError(f"Table {table_name} already exists!")
        if if_exists == "replace":
            conn.executescript(schema)
        elif if_exists == "append":
            _log("Appending...")
        else:
            raise TypeError(
                "`if_exists` has to be one of: 'append', 'replace', or 'fail'."
            )
    else:
        conn.executescript(schema)


# ----------------------------------------------------------------------------


def _generate_schema(name, sql_types):
    cols = []

    max_width = max(
        len(str(cname)) for cnames in sql_types.values() for cname in cnames
    )

    for type_, colnames in sql_types.items():
        colnames_ = [f'"{name}"' for name in colnames]
        cols.extend([f"{name:{max_width+2}} {type_}" for name in colnames_])

    col_lines = ",\n    ".join(cols)

    template = cleandoc(
        """
        DROP TABLE IF EXISTS "{name}";

        CREATE TABLE "{name}" (
            {col_lines}
        );
        """
    )

    return template.format(name=name, col_lines=col_lines)


# ----------------------------------------------------------------------------


def _get_colnames(conn, table_name):
    cursor = conn.execute('SELECT * FROM "' + table_name + '" LIMIT 0')
    names = [description[0] for description in cursor.description]
    return names


# ----------------------------------------------------------------------------


def _get_num_columns(conn, table_name):
    return len(_get_colnames(conn, table_name))


# ----------------------------------------------------------------------------


def _is_int_type(coltype):
    return coltype in [
        int,
        np.int_,
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.uint8,
        np.uint16,
        np.uint32,
        np.uint64,
    ]


# ----------------------------------------------------------------------------


def _log(msg):
    logging.basicConfig(format="%(asctime)s %(message)s", level=logging.INFO)
    logging.info(msg)


# ----------------------------------------------------------------------------


def _not_null(value: Optional[float]):
    return value is not None and not np.isnan(value) and not np.isinf(value)


# ----------------------------------------------------------------------------


def _table_exists(conn, table_name):
    query = (
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
    )

    cursor = conn.execute(query)

    if cursor.fetchone():
        return True
    return False


# ----------------------------------------------------------------------------
