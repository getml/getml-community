# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Factory function for a function that can be used
to iterate through a column.
"""

from typing import Any, Callable, Generator, Type, TypeVar

import numpy as np

ValueType = TypeVar("ValueType", np.float64, bool, str)

CHUNK_SIZE = 100000


def _make_iter(
    value_type: Type[ValueType],
) -> Callable[[Any], Generator[ValueType, None, None]]:
    def _iter(self: Any) -> Generator[ValueType, None, None]:
        length = len(self)
        for begin in range(0, length, CHUNK_SIZE):
            end = min(length, begin + CHUNK_SIZE)
            arr = self[begin:end].to_numpy()
            for val in arr:
                yield value_type(val)

    return _iter
