# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Custom representing a sole feature.
"""

from dataclasses import dataclass
from typing import Sequence

from .sql_string import SQLString


@dataclass
class Feature:
    """
    Dataclass that holds data about a single feature.

    Args:
        index:
            The index of the feature.
        name:
            The name of the feature.
        pipeline:
            The pipeline the feature is from.
        target:
            The target the feature is associated with.
        targets:
            The targets the feature is associated with.
        importance:
            The importance of the feature.
        correlation:
            The correlation of the feature with the target.
        sql:
            The SQL code of the feature.
    """

    index: int
    name: str
    pipeline: str
    target: str
    targets: Sequence[str]
    importance: float
    correlation: float
    sql: SQLString
