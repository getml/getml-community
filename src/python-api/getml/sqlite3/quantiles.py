# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains the quantile aggregations.
"""

from typing import List, Optional

import numpy as np

from .helpers import _not_null

# ----------------------------------------------------------------------------


class _Quantile:
    def __init__(self, quantile: float):
        self.quantile: float = quantile
        self.values: List[float] = []

    def step(self, value: Optional[float]):
        """
        Executed every time the function is called.
        """
        if _not_null(value) and value is not None:
            self.values.append(value)

    def finalize(self) -> float:
        """
        Executed after all values are inserted.
        """
        return np.quantile(self.values, self.quantile, interpolation="linear")  # type:ignore


# ----------------------------------------------------------------------------


class _Q1(_Quantile):
    def __init__(self):
        super().__init__(0.01)


# ----------------------------------------------------------------------------


class _Q5(_Quantile):
    def __init__(self):
        super().__init__(0.05)


# ----------------------------------------------------------------------------


class _Q10(_Quantile):
    def __init__(self):
        super().__init__(0.1)


# ----------------------------------------------------------------------------


class _Q25(_Quantile):
    def __init__(self):
        super().__init__(0.25)


# ----------------------------------------------------------------------------


class _Q75(_Quantile):
    def __init__(self):
        super().__init__(0.75)


# ----------------------------------------------------------------------------


class _Q90(_Quantile):
    def __init__(self):
        super().__init__(0.90)


# ----------------------------------------------------------------------------


class _Q95(_Quantile):
    def __init__(self):
        super().__init__(0.95)


# ----------------------------------------------------------------------------


class _Q99(_Quantile):
    def __init__(self):
        super().__init__(0.99)


# ----------------------------------------------------------------------------
