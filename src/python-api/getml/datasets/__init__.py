# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
The [`datasets`][getml.datasets] module includes utilities to load datasets,
including methods to load and fetch popular reference datasets. It also
features some artificial data generators.
"""

from .base import (
    load_air_pollution,
    load_atherosclerosis,
    load_biodegradability,
    load_consumer_expenditures,
    load_interstate94,
    load_loans,
    load_occupancy,
)
from .samples_generator import (
    _aggregate,
    make_categorical,
    make_discrete,
    make_numerical,
    make_same_units_categorical,
    make_same_units_numerical,
    make_snowflake,
)

__all__ = [
    "load_air_pollution",
    "load_atherosclerosis",
    "load_biodegradability",
    "load_consumer_expenditures",
    "load_interstate94",
    "load_loans",
    "load_occupancy",
    "_aggregate",
    "make_categorical",
    "make_discrete",
    "make_numerical",
    "make_same_units_categorical",
    "make_same_units_numerical",
    "make_snowflake",
]
