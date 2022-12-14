# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
COUNT_ABOVE_MEAN aggregation.
"""

import numpy as np

from .helpers import _not_null


class _CountAboveMean:
    def __init__(self):
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
        if not self.values:
            return 0.0

        # Needed to catch numerical instability
        # issues
        if np.unique(self.values).shape[0] == 1:
            return 0.0

        mean = np.mean(self.values)
        filtered = [0.0 for v in self.values if v > mean]
        return np.float(len(filtered))
