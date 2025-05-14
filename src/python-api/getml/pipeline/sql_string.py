# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Custom str type that holds SQL Source code.
"""


class SQLString(str):
    """
    A custom string type that handles the representation of SQL code strings.
    """

    def _repr_markdown_(self) -> str:
        return "```sql\n" + self + "\n```"
