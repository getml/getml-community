# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns the length of the column
"""

import numpy as np


def _length(self) -> np.int32:
    length = self.length
    if isinstance(length, str):
        raise ValueError(
            "The length is either infinite or cannot "
            + "be known before fully parsing the ColumnView!"
        )
    return length
