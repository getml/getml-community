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
    to all of the data frames that
    were originally passed to .fit(...).
    """

    peripheral: List[Metadata]
    population: Metadata
