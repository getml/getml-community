# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This module contains wrappers around sqlite3 and related utility
functions, which enable you to productionize pipelines using
only sqlite3 and Python, fully based on open-source code.

This requires SQLite version 3.33.0 or above. To check the sqlite3
version of your Python distribution, do the following:

.. code-block:: python

    import sqlite3
    sqlite3.sqlite_version

Example:
    For our example we will assume that you want to productionize
    the CORA project (https://github.com/getml/getml-demo).

    First, we want to transpile the features into SQL code,
    like this:

    .. code-block:: python

        # Set targets to False, if you want an inference pipeline.
        pipe1.features.to_sql(targets=True).save("cora")

    This transpiles the features learned by pipe1 into a set of
    SQLite3 scripts ready to be executed. These scripts are contained
    in a folder called "cora".

    We also assume that you have the three tables needed for
    the CORA project in the form of pandas.DataFrames (other data
    sources are possible).

    We want to create a new sqlite3 connection and then read in the
    data:

    .. code-block:: python

        conn = getml.sqlite3.connect("cora.db")

        getml.sqlite3.read_pandas(
            conn, table_name="cites", data_frame=cites, if_exists="replace")

        getml.sqlite3.read_pandas(
            conn, table_name="content", data_frame=content, if_exists="replace")

        getml.sqlite3.read_pandas(
            conn, table_name="paper", data_frame=paper, if_exists="replace")

    Now we can execute the scripts we have just created:

    .. code-block:: python

        conn = getml.sqlite3.execute(conn, "cora")

    The transpiled pipeline will always create a table called "FEATURES", which
    contain the features. Here is how we retrieve them:

    .. code-block:: python

        features = getml.sqlite3.to_pandas(conn, "FEATURES")

    Now you have created your features in a pandas DataFrame ready to be inserted
    into your favorite machine learning library.

    To build stable data science pipelines, it is often a good idea to ensure
    type safety by hard-coding your table schema. You can use the sniff...
    methods to do that:

    .. code-block:: python

        getml.sqlite3.sniff_pandas("cites", cites)

    This will generate SQLite3 code that creates the "cites" table. You can
    hard-code that into your pipeline. This will ensure that the data always have
    the correct types, avoiding awkward problems in the future.
"""

from .connect import connect
from .execute import execute
from .read_csv import read_csv
from .read_list import read_list
from .read_pandas import read_pandas
from .sniff_csv import sniff_csv
from .sniff_pandas import sniff_pandas
from .to_list import to_list
from .to_pandas import to_pandas
