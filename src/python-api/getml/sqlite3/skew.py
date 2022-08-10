# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
SKEW aggregation.
"""

import numpy as np
from scipy.stats import skew  # type: ignore

from .helpers import _not_null


class _Skew:
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
            return None

        # There appear to be numerical instability
        # issues with the scipy implementation.
        if np.unique(self.values).shape[0] == 1:
            return 0.0

        return skew(self.values)
