# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
MODE aggregation.
"""

import numpy as np
from scipy import stats  # type: ignore

from .helpers import _not_null


class _Mode:
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
        result = stats.mode(self.values, keepdims=True)[0][0]
        try:
            return float(result)
        except ValueError:
            return None
