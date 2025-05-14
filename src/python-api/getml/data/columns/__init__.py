# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Handlers for 1-d arrays storing the data of an individual variable.

Like the [`DataFrame`][getml.DataFrame], the
[`columns`][getml.data.columns] do not contain any actual data themselves
but are only handlers to objects within the getML Engine. These
containers store data of a single variable in a one-dimensional array
of a uniform type.

Columns are *immutable* and *lazily evaluated*.

- *Immutable* means that there are no in-place
  operation on the columns. Any change to the column
  will return a new, changed column.

- *Lazy evaluation* means that operations won't be
  executed until results are required. This is reflected
  in the *column views*: Column views do not exist
  until they are required.

??? example
    This is what some column operations might look like:

    ```python

    import numpy as np

    import getml.data as data
    import getml.engine as engine
    import getml.data.roles as roles

    # ----------------

    engine.set_project("examples")

    # ----------------
    # Create a data frame from a JSON string

    json_str = \"\"\"{
        "names": ["patrick", "alex", "phil", "ulrike"],
        "column_01": [2.4, 3.0, 1.2, 1.4],
        "join_key": ["0", "1", "2", "3"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
    }\"\"\"

    my_df = data.DataFrame(
        "MY DF",
        roles={
            "unused_string": ["names", "join_key", "time_stamp"],
            "unused_float": ["column_01"]}
    ).read_json(
        json_str
    )

    # ----------------

    col1 = my_df["column_01"]

    # ----------------

    # col2 is a column view.
    # The operation is not executed yet.
    col2 = 2.0 - col1

    # This is when '2.0 - col1' is actually
    # executed.
    my_df["column_02"] = col2
    my_df.set_role("column_02", roles.numerical)

    # If you want to update column_01,
    # you can't do that in-place.
    # You need to replace it with a new column
    col1 = col1 + col2
    my_df["column_01"] = col1
    my_df.set_role("column_01", roles.numerical)
    ```
"""

from .columns import (
    BooleanColumnView,
    FloatColumn,
    FloatColumnView,
    StringColumn,
    StringColumnView,
    arange,
    rowid,
)
from .from_value import from_value
from .parse import _parse
from .random import random

__all__ = (
    "BooleanColumnView",
    "FloatColumn",
    "FloatColumnView",
    "StringColumn",
    "StringColumnView",
    "arange",
    "random",
    "rowid",
    "from_value",
    "_parse",
)
