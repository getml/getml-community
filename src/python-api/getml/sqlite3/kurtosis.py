# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
KURTOSIS aggregation.
"""

import numpy as np

from .helpers import _not_null, _try_import_scipy


class _Kurtosis:
    def __init__(self):
        self._scipy = _try_import_scipy()
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
            return None

        # There appear to be numerical instability
        # issues with the scipy implementation.
        if np.unique(self.values).shape[0] == 1:
            return 0.0

        return self._scipy.stats.kurtosis(self.values, fisher=False)
