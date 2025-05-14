# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This module is useful for productionizing pipelines
on Apache Spark.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from getml.sqlite3.execute import _retrieve_scripts
from getml.sqlite3.helpers import _log

if TYPE_CHECKING:
    import pyspark.sql.session


def execute(spark: pyspark.sql.session.SparkSession, fname: str) -> None:
    """
    Executes an SQL script or several SQL scripts on Spark.

    Args:
        spark:
            The spark session.

        fname:
            The names of the SQL script or a folder containing SQL scripts.
            If you decide to pass a folder, the SQL scripts must have the ending '.sql'.
    """
    # ------------------------------------------------------------

    if not isinstance(fname, str):
        raise TypeError("'fname' must be of type str")

    # ------------------------------------------------------------

    if os.path.isdir(fname):
        scripts = _retrieve_scripts(fname, ".sql")
        for script in scripts:
            execute(spark, script)
        return

    # ------------------------------------------------------------

    _log("Executing " + fname + "...")

    with open(fname, encoding="utf-8") as sql_files:
        queries = sql_files.read().split(";")

    for query in queries:
        if query.strip():
            spark.sql(query)
