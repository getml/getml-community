# Copyright 2022 The SQLNet Company GmbH
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
    """

    name: str
    roles: Roles


class AllMetadata(NamedTuple):
    """
    Contains the metadata related
    to all the data frames that
    were originally passed to .fit(...).
    """

    peripheral: List[Metadata]
    population: Metadata
