# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
NUM_MAX aggregation.
"""

import numpy as np

from .helpers import _not_null


class _NumMax:
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
        maximum = np.max(self.values)
        filtered = [0.0 for v in self.values if v == maximum]
        return np.float(len(filtered))
