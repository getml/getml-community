# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
COUNT_DISTINCT_OVER_COUNT aggregation.
"""

import numpy as np

from .helpers import _not_null


class _CountDistinctOverCount:
    def __init__(self):
        self.count = 0.0
        self.values = []

    def step(self, value):
        """
        Executed every time the function is called.
        """
        self.count += 1.0
        if _not_null(value):
            self.values.append(value)

    def finalize(self):
        """
        Executed after all values are inserted.
        """
        if self.count == 0:
            return None
        count_distinct = np.float(len(np.unique(self.values)))
        return count_distinct / self.count
