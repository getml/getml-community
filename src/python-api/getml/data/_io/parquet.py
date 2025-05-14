# Copyright 2025 Code17 GmbH
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
from typing import TYPE_CHECKING, Iterable, Optional, Union

import pyarrow.parquet as pq

from getml.data._io.arrow import sniff_schema
from getml.data.roles.container import Roles

if TYPE_CHECKING:
    from getml.data.data_frame import DataFrame
    from getml.data.view import View


def sniff_parquet(fnames: Iterable[str], colnames: Iterable[str]) -> Roles:
    first_fname = next(iter(fnames))
    schema = pq.read_schema(first_fname)

    for fname in it.islice(fnames, 1, None):
        if pq.read_schema(fname) != schema:
            raise ValueError(
                "All Parquet files must have the same schema."
                f"Expected: {schema} (from {first_fname!r}) got {pq.read_schema(fname)} (from {fname!r})"
            )

    return sniff_schema(schema, colnames)


def to_parquet(
    df_or_view: Union[DataFrame, View],
    fname: str,
    compression: str,
    coerce_timestamps: Optional[bool] = None,
):
    df_or_view.refresh()

    if not isinstance(fname, str):
        raise TypeError("'fname' must be of type str")

    if not isinstance(compression, str):
        raise TypeError("'compression' must be of type str")

    sink = Path(fname)

    with df_or_view.to_arrow_stream() as reader:
        with pq.ParquetWriter(
            sink,
            reader.schema,
            compression=compression,
            coerce_timestamps=coerce_timestamps,
        ) as writer:
            for batch in reader:
                writer.write_batch(batch)
