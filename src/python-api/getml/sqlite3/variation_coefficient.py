# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
VARIATION_COEFFICIENT aggregation.
"""

import numpy as np

from .helpers import _not_null


class _VariationCoefficient:
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
        mean = np.mean(self.values)
        if mean == 0.0:
            return None
        return np.var(self.values) / mean
