# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Class representing metadata on a column used by the features of a pipeline.
"""

from dataclasses import dataclass

import numpy as np


@dataclass
class Column:
    """
    Dataclass that holds data about a single column.
    """

    index: int
    name: str
    marker: str
    table: str
    target: str
    importance: float = np.nan
