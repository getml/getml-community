# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from typing import FrozenSet, get_args

from getml.data.roles import roles
from getml.data.roles.types import CategoricalLike, NumericalLike, Role, Target

_target: FrozenSet[Target] = frozenset({roles.target})

categorical: FrozenSet[CategoricalLike] = frozenset(
    {
        roles.categorical,
        roles.join_key,
        roles.text,
        roles.unused_string,
    }
)

numerical: FrozenSet[NumericalLike] = frozenset(
    {
        roles.numerical,
        roles.time_stamp,
        roles.unused_float,
    }
)

all_: FrozenSet[Role] = categorical | numerical | _target