# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""This module contains communication routines to access various databases.

The :func:`~getml.database.connect_bigquery`,
:func:`~getml.database.connect_hana`,
:func:`~getml.database.connect_greenplum`,
:func:`~getml.database.connect_mariadb`,
:func:`~getml.database.connect_mysql`,
:func:`~getml.database.connect_postgres`, and
:func:`~getml.database.connect_sqlite3` functions establish a
connection between a database and the getML engine. During the data
import using either the :meth:`~getml.DataFrame.read_db` or
:meth:`~getml.DataFrame.read_query` methods of a
:class:`~getml.DataFrame` instance or the corresponding
:func:`~getml.DataFrame.from_db` class method all data will be
directly loaded from the database into the engine without ever passing
the Python interpreter.

In addition, several auxiliary functions that might be handy during
the analysis and interaction with the database are provided.
"""

from .helpers import _load_to_buffer, _retrieve_temp_dir, _retrieve_url, _retrieve_urls
from .connect_bigquery import connect_bigquery
from .connect_greenplum import connect_greenplum
from .connect_hana import connect_hana
from .connect_mariadb import connect_mariadb
from .connect_mysql import connect_mysql
from .connect_odbc import connect_odbc
from .connect_postgres import connect_postgres
from .connect_sqlite3 import connect_sqlite3
from .connection import Connection
from .copy_table import copy_table
from .drop_table import drop_table
from .execute import execute
from .get import get
from .get_colnames import get_colnames
from .list_connections import list_connections
from .list_tables import list_tables
from .read_csv import read_csv
from .read_s3 import read_s3
from .sniff_csv import sniff_csv
from .sniff_s3 import sniff_s3

__all__ = (
    "Connection",
    "connect_bigquery",
    "connect_greenplum",
    "connect_hana",
    "connect_mariadb",
    "connect_mysql",
    "connect_odbc",
    "connect_postgres",
    "connect_sqlite3",
    "copy_table",
    "drop_table",
    "execute",
    "get",
    "get_colnames",
    "list_connections",
    "list_tables",
    "read_csv",
    "read_s3",
    "sniff_csv",
    "sniff_s3",
)
