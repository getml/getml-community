# Copyright 2022 The SQLNet Company GmbH
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
    """

    index: int
    name: str
    pipeline: str
    target: str
    targets: Sequence[str]
    importance: float
    correlation: float
    sql: SQLString
