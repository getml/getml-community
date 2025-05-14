# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
COUNT_BELOW_MEAN aggregation.
"""

from typing import List, Optional

import numpy as np

from .helpers import _not_null


class _CountBelowMean:
    def __init__(self):
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
        if not self.values:
            return 0.0

        # Needed to catch numerical instability
        # issues
        if np.unique(self.values).shape[0] == 1:
            return 0.0

        mean = np.mean(self.values)
        filtered = [0.0 for v in self.values if v < mean]
        return float(len(filtered))
