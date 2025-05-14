# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains the exponentially weighted moving average aggregations.
"""

from typing import Optional

import numpy as np

from .helpers import _not_null

# ----------------------------------------------------------------------------


class _EWMATrend:
    log05 = np.log(0.5)

    t1s = 1.0
    t1m = t1s * 60.0
    t1h = t1m * 60.0
    t1d = t1h * 24.0
    t7d = t1d * 7.0
    t30d = t1d * 30.0
    t90d = t1d * 90.0
    t365d = t1d * 365.0

    def __init__(self, half_life):
        self.half_life = half_life
        self.values = []

    def step(self, value, time_stamp):
        """
        Executed every time the function is called.
        """
        if _not_null(value) and _not_null(time_stamp):
            self.values.append((time_stamp, value))

    def finalize(self) -> Optional[float]:
        """
        Executed after all values are inserted.
        """
        weights = [
            np.exp(_EWMATrend.log05 * v[0] / self.half_life) for v in self.values
        ]
        sum_weights = np.sum(weights)
        if sum_weights == 0.0:
            return None
        mean_x = sum((v[0] * w for v, w in zip(self.values, weights))) / sum_weights
        mean_y = sum((v[1] * w for v, w in zip(self.values, weights))) / sum_weights
        sum_xx = sum(
            (
                (v[0] - mean_x) * (v[0] - mean_x) * w
                for v, w in zip(self.values, weights)
            )
        )
        if sum_xx == 0.0:
            return mean_y
        sum_xy = sum(
            (
                (v[0] - mean_x) * (v[1] - mean_y) * w
                for v, w in zip(self.values, weights)
            )
        )
        beta = sum_xy / sum_xx
        return mean_y - beta * mean_x


# ----------------------------------------------------------------------------


class _EWMATrend1S(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t1s)


# ----------------------------------------------------------------------------


class _EWMATrend1M(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t1m)


# ----------------------------------------------------------------------------


class _EWMATrend1H(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t1h)


# ----------------------------------------------------------------------------


class _EWMATrend1D(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t1d)


# ----------------------------------------------------------------------------


class _EWMATrend7D(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t7d)


# ----------------------------------------------------------------------------


class _EWMATrend30D(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t30d)


# ----------------------------------------------------------------------------


class _EWMATrend90D(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t90d)


# ----------------------------------------------------------------------------


class _EWMATrend365D(_EWMATrend):
    def __init__(self):
        super().__init__(_EWMATrend.t365d)


# ----------------------------------------------------------------------------
