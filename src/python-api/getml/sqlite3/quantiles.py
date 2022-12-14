# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains the quantile aggregations.
"""

import numpy as np

from .helpers import _not_null

# ----------------------------------------------------------------------------


class _Quantile:
    def __init__(self, quantile):
        self.quantile = quantile
        self.values = []

    def step(self, value):
        """
        Executed every time the function is called.
        """
        if _not_null(value):
            self.values.append(value)

    def finalize(self):
        """
        Executed after all values are inserted.
        """
        return np.quantile(self.values, self.quantile, interpolation="linear")


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
