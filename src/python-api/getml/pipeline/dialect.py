# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
SQL dialects that can be used for the generated code.

One way to productionize a :class:`~getml.Pipeline` is
to transpile its features to production-ready SQL code.
This SQL code can be run on standard cloud infrastructure.
Please also refer to :class:`~getml.pipeline.SQLCode`.

Example:
    .. code-block:: python

        sql_code = my_pipeline.features.to_sql(
            getml.pipeline.dialect.spark_sql)

        # Creates a folder called "my_pipeline"
        # which contains the SQL scripts.
        sql_code.save("my_pipeline")
"""

import re
from typing import Pattern

# --------------------------------------------------------------

_all_dialects = [
    "bigquery",
    "human-readable sql",
    "mysql",
    "postgres",
    "spark sql",
    "sqlite3",
    "tsql",
]

# --------------------------------------------------------------

bigquery = _all_dialects[0]
"""BigQuery is a proprietary database system used by the Google Cloud.

Note:
    Not supported in the getML community edition.
"""

human_readable_sql = _all_dialects[1]
"""SQL that is not meant to be executed, but for interpretation by humans.
"""

mysql = _all_dialects[2]
"""MySQL and its fork MariaDB are among the most popular open-source
database systems.

Note:
    Not supported in the getML community edition.
"""

postgres = _all_dialects[3]
"""The PostgreSQL or postgres dialect is a popular SQL dialect
used by PostgreSQL and its many derivatives like Redshift
or Greenplum.

Note:
    Not supported in the getML community edition.
"""

spark_sql = _all_dialects[4]
"""Spark SQL is the SQL dialect used by Apache Spark.

Apache Spark is an open-source, distributed, in-memory
engine for large-scale data processing and a popular
choice for producutionizing machine learning pipelines.

Note:
    Not supported in the getML community edition.
"""

sqlite3 = _all_dialects[5]
"""SQLite3 is a light-weight and widely used database system.

It is recommended for live prediction systems or when the amount
of data handled is unlikely to be too large.

Note:
    Not supported in the getML community edition.
"""

tsql = _all_dialects[6]
"""TSQL or Transact-SQL is the dialect used by most Microsoft
databases.

Note:
    Not supported in the getML community edition.
"""


# --------------------------------------------------------------


def _drop_table(dialect: str, key: str) -> str:
    if dialect in (bigquery, mysql, spark_sql):
        return "DROP TABLE IF EXISTS `" + key.upper() + "`"

    if dialect in (human_readable_sql, postgres, sqlite3):
        return 'DROP TABLE IF EXISTS "' + key.upper() + '"'

    if dialect == tsql:
        return "DROP TABLE IF EXISTS \[" + key.upper() + "\]"

    raise ValueError(
        "Unknown dialect: '"
        + dialect
        + "'. Please choose one of the following: "
        + str(_all_dialects)
    )


# --------------------------------------------------------------


def _table_pattern(dialect: str) -> Pattern:
    if dialect in (bigquery, mysql, spark_sql):
        return re.compile("CREATE TABLE `(.+)`")

    if dialect in (human_readable_sql, postgres, sqlite3):
        return re.compile('CREATE TABLE "(.+)"')

    if dialect == tsql:
        return re.compile("INTO \[(.+)\]")

    raise ValueError(
        "Unknown dialect: '"
        + dialect
        + "'. Please choose one of the following: "
        + str(_all_dialects)
    )


# --------------------------------------------------------------
