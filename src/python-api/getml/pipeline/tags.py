# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A small lists-type class that allows to search for arbitrary substrings within its items.
"""


class Tags(list):
    """
    A small lists-type class that allows to search for arbitrary substrings within its items.
    """

    def __contains__(self, substr: object) -> bool:
        if not isinstance(substr, str):
            raise ValueError("Tags can only contain strings.")
        return any(substr in tag for tag in self)
