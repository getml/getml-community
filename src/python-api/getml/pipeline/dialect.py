# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
SQL dialects that can be used for the generated code.

One way to productionize a [`Pipeline`][getml.Pipeline] is
to transpile its features to production-ready SQL code.
This SQL code can be run on standard cloud infrastructure.
Please also refer to [`SQLCode`][getml.pipeline.SQLCode].

??? example
    ```python
    sql_code = my_pipeline.features.to_sql(
        getml.pipeline.dialect.spark_sql)

    # Creates a folder called "my_pipeline"
    # which contains the SQL scripts.
    sql_code.save("my_pipeline")
    ```
"""

import re
from typing import Pattern

# --------------------------------------------------------------

_all_dialects = [
    "bigquery",
    "duckdb",
    "human-readable sql",
    "mysql",
    "postgres",
    "spark sql",
    "sqlite3",
    "tsql",
]

# --------------------------------------------------------------

bigquery = _all_dialects[0]
"""
BigQuery is a proprietary database system used by the Google Cloud.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""

duckdb = _all_dialects[1]
"""
DuckDB is an columnar database system that is designed for OLAP workloads.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""


human_readable_sql = _all_dialects[2]
"""
SQL that is not meant to be executed, but for interpretation by humans.
"""

mysql = _all_dialects[3]
"""
MySQL and its fork MariaDB are among the most popular open-source
database systems.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""

postgres = _all_dialects[4]
"""
The PostgreSQL or postgres dialect is a popular SQL dialect
used by PostgreSQL and its many derivatives like Redshift
or Greenplum.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""

spark_sql = _all_dialects[5]
"""
Spark SQL is the SQL dialect used by Apache Spark.

Apache Spark is an open-source, distributed, in-memory
engine for large-scale data processing and a popular
choice for productionizing machine learning pipelines.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""

sqlite3 = _all_dialects[6]
"""
SQLite3 is a light-weight and widely used database system.

It is recommended for live prediction systems or when the amount
of data handled is unlikely to be too large.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""

tsql = _all_dialects[7]
"""
TSQL or Transact-SQL is the dialect used by most Microsoft
databases.

enterprise-adm: Enterprise edition
    This feature is exclusive to the Enterprise edition and is not available in the Community edition. Discover the [benefits of the Enterprise edition][enterprise-benefits] and [compare their features][enterprise-feature-list].

    For licensing information and technical support, please [contact us][contact-page].
"""


# --------------------------------------------------------------


def _drop_table(dialect: str, key: str) -> str:
    if dialect in (bigquery, mysql, spark_sql):
        return f"DROP TABLE IF EXISTS `{key.upper()}`"

    if dialect in (duckdb, human_readable_sql, postgres, sqlite3):
        return 'DROP TABLE IF EXISTS "' + key.upper() + '"'

    if dialect == tsql:
        return f"DROP TABLE IF EXISTS [{key.upper()}]"

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

    if dialect in (duckdb, human_readable_sql, postgres, sqlite3):
        return re.compile('CREATE TABLE "(.+)"')

    if dialect == tsql:
        return re.compile(r"INTO \[(.+)\]")

    raise ValueError(
        "Unknown dialect: '"
        + dialect
        + "'. Please choose one of the following: "
        + str(_all_dialects)
    )


# --------------------------------------------------------------
