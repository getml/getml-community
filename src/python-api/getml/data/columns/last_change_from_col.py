# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
The last time any of the underlying data frames has been changed.
"""

from .last_change import _last_change


@property  # type: ignore
def _last_change_from_col(self):
    """
    The last time any of the underlying data frames has been changed.
    """

    def _last_change_from_cmd(cmd):
        if cmd["type_"] == "FloatColumn" or cmd["type_"] == "StringColumn":
            assert "df_name_" in cmd, "Expected df_name_"
            return _last_change(cmd["df_name_"])

        def get_operand(op):
            return [_last_change_from_cmd(cmd[op])] if op in cmd else []

        all_values = (
            [""]
            + get_operand("operand1_")
            + get_operand("operand2_")
            + get_operand("condition_")
        )

        return max(all_values)

    return _last_change_from_cmd(self.cmd)
