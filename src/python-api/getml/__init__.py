# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
getML (https://getml.com) is a software for automated machine learning
(AutoML) with a special focus on feature learning for relational data
and time series. The getML algorithms can produce features that are far
more advanced than what any data scientist could write by hand or what you
could accomplish using simple brute force approaches.

This is the official python client for the getML engine.

Documentation and more details at https://docs.getml.com
"""

from . import (
    communication,
    constants,
    data,
    database,
    datasets,
    engine,
    feature_learning,
    hyperopt,
    pipeline,
    predictors,
    preprocessors,
    project,
    sqlite3,
    spark,
    utilities,
)
from .cross_validation import cross_validation
from .data import DataFrame
from .pipeline import Pipeline
from .engine import set_project
from .version import __version__

# Import subpackages into top-level namespace


__all__ = (
    "__version__",
    "communication",
    "constants",
    "cross_validation",
    "data",
    "database",
    "datasets",
    "engine",
    "feature_learning",
    "hyperopt",
    "pipeline",
    "predictors",
    "preprocessors",
    "project",
    "spark",
    "sqlite3",
    "utilities",
    "DataFrame",
    "Pipeline",
    "set_project",
)
