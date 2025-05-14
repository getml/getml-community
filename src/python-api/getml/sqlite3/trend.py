# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Extracts linear trends over time.
"""

import numpy as np

from .helpers import _not_null

# ----------------------------------------------------------------------------


class _Trend:
    def __init__(self):
        self.values = []

    def step(self, value, time_stamp):
        """
        Executed every time the function is called.
        """
        if _not_null(value):
            self.values.append((time_stamp, value))

    def finalize(self):
        """
        Executed after all values are inserted.
        """
        if not self.values:
            return None
        mean_x = np.mean([v[0] for v in self.values])
        mean_y = np.mean([v[1] for v in self.values])
        sum_xx = np.sum([(v[0] - mean_x) * (v[0] - mean_x) for v in self.values])
        if sum_xx == 0.0:
            return mean_y
        sum_xy = np.sum([(v[0] - mean_x) * (v[1] - mean_y) for v in self.values])
        beta = sum_xy / sum_xx
        return mean_y - beta * mean_x


# ----------------------------------------------------------------------------
