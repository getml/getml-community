# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import Literal

"""
The Literal types that define all possible roles a column can have.

See [`getml.data.roles.roles`][getml.data.roles.roles] for the actual constants.
"""

Categorical = Literal["categorical"]
JoinKey = Literal["join_key"]
Numerical = Literal["numerical"]
Target = Literal["target"]
Text = Literal["text"]
TimeStamp = Literal["time_stamp"]
UnusedFloat = Literal["unused_float"]
UnusedString = Literal["unused_string"]

CategoricalLike = Literal[Categorical, JoinKey, Text, UnusedString]
NumericalLike = Literal[Numerical, Target, TimeStamp, UnusedFloat]

Role = Literal[
    Categorical,
    JoinKey,
    Numerical,
    Target,
    Text,
    TimeStamp,
    UnusedFloat,
    UnusedString,
]
