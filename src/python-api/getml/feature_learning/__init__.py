# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""This module contains relational learning algorithms to learn features
from relational data or time series.

Note:

    All feature learners need to be passed to :class:`~getml.Pipeline`.


"""

from .fastprop import FastProp
from .feature_learner import _FeatureLearner
from .multirel import Multirel
from .relboost import Relboost
from .relmt import RelMT

__all__ = (
    "_FeatureLearner",
    "FastProp",
    "Multirel",
    "Relboost",
    "RelMT",
    "aggregations",
    "loss_functions",
)
