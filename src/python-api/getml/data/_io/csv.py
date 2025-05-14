# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Utilities for working with CSV files.
"""

from __future__ import annotations

import itertools as it
import numbers
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Iterator, List, Tuple, Union

import pyarrow as pa
import pyarrow.csv as pa_csv

from getml.constants import DEFAULT_BATCH_SIZE
from getml.data._io.arrow import preprocess_arrow_schema, sniff_schema
from getml.data.roles.container import Roles
from getml.database.helpers import _retrieve_urls

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame
    from getml.data.view import View

DEFAULT_CSV_READ_BLOCK_SIZE = 1 << 20


def sniff_csv(
    fnames: List[str],
    quotechar: str = '"',
    sep: str = ",",
    skip: int = 0,
    colnames: Iterable[str] = (),
) -> Roles:
    """Sniffs a list of CSV files and returns the result as a dictionary of
    roles.

    Args:
        fnames: The list of CSV file names to be read.

        num_lines_sniffed:

            Number of lines analysed by the sniffer.

        quotechar:

            The character used to wrap strings.

        sep:

            The character used for separating fields.

        skip:
            Number of lines to skip at the beginning of each file.

        colnames: The first line of a CSV file
            usually contains the column names. When this is not the case, you need to
            explicitly pass them.

    Returns:
        Keyword arguments (kwargs) that can be used to construct a DataFrame.
    """

    fnames_ = _retrieve_urls(fnames, verbose=False)

    readers = [
        pa_csv.open_csv(
            fname,
            read_options=pa_csv.ReadOptions(
                skip_rows=skip,
                column_names=colnames,
            ),
            parse_options=pa_csv.ParseOptions(
                delimiter=sep,
                quote_char=quotechar,
            ),
        )
        for fname in fnames_
    ]

    schema, *deviating = set(reader.schema for reader in readers)

    if deviating:
        raise ValueError("All CSV files must have the same schema!")

    if not colnames:
        colnames = schema.names

    return sniff_schema(schema, colnames)


def to_csv(
    df_or_view: Union[DataFrame, View],
    fname: str,
    sep: str = ",",
    batch_size: int = DEFAULT_BATCH_SIZE,
    quoting_style: str = "needed",
):
    df_or_view.refresh()

    if not isinstance(fname, str):
        raise TypeError("'fname' must be of type str")

    if not isinstance(sep, str):
        raise TypeError("'sep' must be of type str")

    if not isinstance(batch_size, numbers.Real):
        raise TypeError("'batch_size' must be a real number")

    if batch_size == 0:
        batch_size = len(df_or_view)

    sink = Path(fname)

    write_options = pa_csv.WriteOptions(
        batch_size=batch_size,
        delimiter=sep,
        quoting_style=quoting_style,
    )

    batches = (batch.to_arrow() for batch in df_or_view.iter_batches(batch_size))

    first_batch = next(batches)
    schema = first_batch.schema

    writer = pa_csv.CSVWriter(sink, schema, write_options=write_options)

    for batch in it.chain([first_batch], batches):
        writer.write(batch)


def _prepare_csv_options(
    input_file: PathLike,
    roles: Roles,
    skip_rows: int,
    column_names: Iterable[str],
    delimiter: str,
    quote_char: str,
    block_size: int,
) -> Tuple[pa_csv.ReadOptions, pa_csv.ParseOptions, pa_csv.ConvertOptions]:
    read_options = pa_csv.ReadOptions(
        block_size=block_size,
        skip_rows=skip_rows,
        column_names=column_names,
        autogenerate_column_names=False,
    )

    parse_options = pa_csv.ParseOptions(
        delimiter=delimiter,
        quote_char=quote_char,
    )

    preprocessed_schema = preprocess_arrow_schema(
        pa_csv.open_csv(input_file).schema, roles
    )
    convert_options = pa_csv.ConvertOptions(
        column_types=preprocessed_schema,
    )

    return read_options, parse_options, convert_options


def stream_csv(
    input_file: PathLike,
    roles: Roles,
    skip_rows: int = 0,
    column_names: Iterable[str] = (),
    delimiter: str = ",",
    quote_char: str = '"',
    block_size: int = DEFAULT_CSV_READ_BLOCK_SIZE,
) -> pa_csv.CSVStreamingReader:
    read_options, parse_options, convert_options = _prepare_csv_options(
        input_file,
        roles,
        skip_rows,
        column_names,
        delimiter,
        quote_char,
        block_size,
    )

    return pa_csv.open_csv(
        input_file,
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options,
    )


def read_csv(
    input_file: PathLike,
    roles: Roles,
    skip_rows: int = 0,
    column_names: Iterable[str] = (),
    delimiter: str = ",",
    quote_char: str = '"',
    block_size: int = DEFAULT_CSV_READ_BLOCK_SIZE,
) -> Iterator[pa.RecordBatch]:
    read_options, parse_options, convert_options = _prepare_csv_options(
        input_file,
        roles,
        skip_rows,
        column_names,
        delimiter,
        quote_char,
        block_size,
    )

    return iter(
        pa_csv.read_csv(
            input_file,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options,
        ).to_batches()
    )
