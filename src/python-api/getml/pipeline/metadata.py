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
    to the data frames that were originally
    passed to .fit(...).
    """

    peripheral: List[Roles]
    population: Roles
