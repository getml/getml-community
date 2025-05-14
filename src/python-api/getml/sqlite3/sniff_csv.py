# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains utility functions for sniffing sqlite data types from CSV files.
"""

from typing import List, Optional, Union

import pandas as pd

from getml.data.helpers import _is_non_empty_typed_list, _is_typed_list

from .sniff_pandas import sniff_pandas

# ----------------------------------------------------------------------------


def sniff_csv(
    fnames: Union[str, List[str]],
    table_name: str,
    header: bool = True,
    num_lines_sniffed: int = 1000,
    quotechar: str = '"',
    sep: str = ",",
    skip: int = 0,
    colnames: Optional[List[str]] = None,
) -> str:
    """
    Sniffs a list of csv files.

    Args:
        fnames:
            The list of CSV file names to be read.

        table_name:
            Name of the table in which the data is to be inserted.

        header:
            Whether the csv file contains a header. If True, the first line
            is skipped and column names are inferred accordingly.

        num_lines_sniffed:
            Number of lines analyzed by the sniffer.

        quotechar:
            The character used to wrap strings.

        sep:
            The separator used for separating fields.

        skip:
            Number of lines to skip at the beginning of each
            file.

        colnames:
            The first line of a CSV file
            usually contains the column names. When this is not the case, you can
            explicitly pass them. If you pass colnames, it is assumed that the
            CSV files do not contain a header, thus overriding the 'header' variable.

    Returns:
            Appropriate `CREATE TABLE` statement.
    """

    # ------------------------------------------------------------

    if not isinstance(fnames, list):
        fnames = [fnames]

    # ------------------------------------------------------------

    if not _is_non_empty_typed_list(fnames, str):
        raise TypeError("'fnames' must be a string or a non-empty list of strings")

    if not isinstance(table_name, str):
        raise TypeError("'table_name' must be a string")

    if not isinstance(header, bool):
        raise TypeError("'header' must be a bool")

    if not isinstance(num_lines_sniffed, int):
        raise TypeError("'num_lines_sniffed' must be a int")

    if not isinstance(quotechar, str):
        raise TypeError("'quotechar' must be a str")

    if not isinstance(sep, str):
        raise TypeError("'sep' must be a str")

    if not isinstance(skip, int):
        raise TypeError("'skip' must be an int")

    if colnames is not None and not _is_typed_list(colnames, str):
        raise TypeError("'colnames' must be a list of strings or None")

    # ------------------------------------------------------------

    header_lines = 0 if header and not colnames else None

    def read(fname):
        return pd.read_csv(
            fname,
            nrows=num_lines_sniffed,
            header=header_lines,
            sep=sep,
            quotechar=quotechar,
            skiprows=skip,
            names=colnames,
        )

    data_frames = [read(fname) for fname in fnames]

    merged = pd.concat(data_frames, join="inner")

    return sniff_pandas(table_name, merged)
