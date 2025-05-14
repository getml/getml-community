# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""This module contains communication routines to access various databases.

The [`connect_bigquery`][getml.database.connect_bigquery.connect_bigquery],
[`connect_hana`][getml.database.connect_hana.connect_hana],
[`connect_greenplum`][getml.database.connect_greenplum.connect_greenplum],
[`connect_mariadb`][getml.database.connect_mariadb.connect_mariadb],
[`connect_mysql`][getml.database.connect_mysql.connect_mysql],
[`connect_postgres`][getml.database.connect_postgres.connect_postgres], and
[`connect_sqlite3`][getml.database.connect_sqlite3.connect_sqlite3] functions establish a
connection between a database and the getML Engine. During the data
import using either the [`read_db`][getml.DataFrame.read_db] or
[`read_query`][getml.DataFrame.read_query] methods of a
[`DataFrame`][getml.DataFrame] instance or the corresponding
[`from_db`][getml.DataFrame.from_db] class method all data will be
directly loaded from the database into the Engine without ever passing
the Python interpreter.

In addition, several auxiliary functions that might be handy during
the analysis and interaction with the database are provided.

"""

import getml.database.exceptions
from getml.database.connect_bigquery import connect_bigquery
from getml.database.connect_duckdb import connect_duckdb
from getml.database.connect_greenplum import connect_greenplum
from getml.database.connect_hana import connect_hana
from getml.database.connect_mariadb import connect_mariadb
from getml.database.connect_mysql import connect_mysql
from getml.database.connect_odbc import connect_odbc
from getml.database.connect_postgres import connect_postgres
from getml.database.connect_sqlite3 import connect_sqlite3
from getml.database.connection import Connection
from getml.database.copy_table import copy_table
from getml.database.drop_table import drop_table
from getml.database.execute import execute
from getml.database.get import get
from getml.database.get_colnames import get_colnames
from getml.database.helpers import _retrieve_temp_dir, _retrieve_url, _retrieve_urls
from getml.database.list_connections import list_connections
from getml.database.list_tables import list_tables
from getml.database.read_csv import read_csv
from getml.database.read_s3 import read_s3
from getml.database.sniff_csv import sniff_csv
from getml.database.sniff_s3 import sniff_s3

__all__ = (
    "Connection",
    "connect_bigquery",
    "connect_duckdb",
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
    "_retrieve_temp_dir",
    "_retrieve_url",
    "_retrieve_urls",
    "list_connections",
    "list_tables",
    "read_csv",
    "read_s3",
    "sniff_csv",
    "sniff_s3",
)
