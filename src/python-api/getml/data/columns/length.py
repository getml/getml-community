# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns the length of the column
"""

import numpy as np


def _length(col) -> int:
    length = col.length
    if isinstance(length, str):
        raise ValueError(
            "The length is either infinite or cannot "
            + "be known before fully parsing the ColumnView!"
        )
    return length
