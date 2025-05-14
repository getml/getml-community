# Copyright 2025 Code17 GmbH
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

    Args:
        index:
            The index of the column.
        name:
            The name of the column.
        marker:
            The marker of the column.
        table:
            The table the column is from.
        target:
            The target the column is associated with.
        importance:
            The importance of the column.
    """

    index: int
    name: str
    marker: str
    table: str
    target: str
    importance: float = np.nan
