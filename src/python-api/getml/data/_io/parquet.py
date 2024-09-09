# Copyright 2024 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Utitlities for working with Parquet files.
"""

from __future__ import annotations

import itertools as it
from pathlib import Path
from typing import TYPE_CHECKING, Iterable, Union

import pyarrow.parquet as pq

from getml.constants import DEFAULT_BATCH_SIZE
from getml.data._io.arrow import sniff_schema
from getml.data.roles.container import Roles

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame
    from getml.data.view import View


def sniff_parquet(fnames: Iterable[str], colnames: Iterable[str]) -> Roles:
    schema, *deviating = set(pq.read_schema(fname) for fname in fnames)
    if deviating:
        raise ValueError("All Parquet files must have the same schema!")
    return sniff_schema(schema, colnames)


def to_parquet(
    df_or_view: Union[DataFrame, View],
    fname: str,
    compression: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
):
    df_or_view.refresh()

    if not isinstance(fname, str):
        raise TypeError("'fname' must be of type str")

    if not isinstance(compression, str):
        raise TypeError("'compression' must be of type str")

    sink = Path(fname)

    batches = (batch.to_arrow() for batch in df_or_view.iter_batches(batch_size))

    first_batch = next(batches)
    schema = first_batch.schema

    writer = pq.ParquetWriter(
        sink,
        schema,
        compression=compression,
    )

    for batch in it.chain([first_batch], batches):
        writer.write(batch)
