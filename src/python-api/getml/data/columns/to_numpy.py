# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Transform column to a numpy array.
"""

from typing import Any

import numpy as np

from .to_arrow import _to_arrow


def _to_numpy(self: Any) -> np.ndarray:
    """
    Transform column to numpy.ndarray
    """
    return _to_arrow(self).to_numpy()
