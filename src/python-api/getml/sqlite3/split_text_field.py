# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Splits a text field into its individual components.
This can not be called directly from SQLite3. It is
a helper function.
"""

SEPARATORS = ";,.!?-|\t\"\v\f\r\n%'()[]}{"


def _split_text_field(textfield):
    lower = textfield.lower()
    for sep in SEPARATORS:
        lower = lower.replace(sep, " ")
    splitted = lower.split(" ")
    return [s for s in splitted if s]
