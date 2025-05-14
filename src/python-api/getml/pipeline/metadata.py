# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Contains the metadata related
to the data frames that were originally
passed to .fit(...).
"""

from typing import List, NamedTuple

from getml.data import Roles


class Metadata(NamedTuple):
    """
    Contains the metadata related
    to a data frame that
    were originally passed to .fit(...).

    Attributes:
        name:
            The name of the data frame.
        roles:
            The roles of the columns in the data frame.
    """

    name: str
    roles: Roles


class AllMetadata(NamedTuple):
    """
    Contains the metadata related
    to all the data frames that
    were originally passed to .fit(...).

    Attributes:
        peripheral:
            The metadata of the peripheral tables.
        population:
            The metadata of the population table.
    """

    peripheral: List[Metadata]
    population: Metadata
