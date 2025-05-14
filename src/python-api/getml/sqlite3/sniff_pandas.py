# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains utility functions for siffing sqlite data types from pandas DataFrames.
"""

from typing import Dict, List

import pandas as pd

from getml.data.helpers import _is_numerical_type_numpy

from .helpers import _generate_schema, _is_int_type

# ----------------------------------------------------------------------------


def sniff_pandas(table_name: str, data_frame: pd.DataFrame) -> str:
    """
    Sniffs a pandas data frame.

    Args:
        table_name:
            Name of the table in which the data is to be inserted.

        data_frame:
            The pandas.DataFrame to read into the table.

    Returns:
            Appropriate `CREATE TABLE` statement.
    """
    # ------------------------------------------------------------

    if not isinstance(table_name, str):
        raise TypeError("'table_name' must be a str")

    if not isinstance(data_frame, pd.DataFrame):
        raise TypeError("'data_frame' must be a pandas.DataFrame")

    # ------------------------------------------------------------

    colnames = data_frame.columns
    coltypes = data_frame.dtypes

    sql_types: Dict[str, List[str]] = {"INTEGER": [], "REAL": [], "TEXT": []}

    for cname, ctype in zip(colnames, coltypes):
        if _is_int_type(ctype):
            sql_types["INTEGER"].append(cname)
            continue
        if _is_numerical_type_numpy(ctype):
            sql_types["REAL"].append(cname)
        else:
            sql_types["TEXT"].append(cname)

    return _generate_schema(table_name, sql_types)
