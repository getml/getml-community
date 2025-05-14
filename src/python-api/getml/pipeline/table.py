# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Contains class representing data for a table of a pipeline.
"""

from dataclasses import dataclass


@dataclass
class Table:
    """
    A dataclass that holds data about a single table.

    Args:
        name:
            The name of the table.
        importance:
            The importance of the table.
        target:
            The target of the table.
        marker:
            The marker of the table.
    """

    name: str
    importance: float
    target: str
    marker: str
