# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Returns the word indicated by i from the textfield.
Note that this begins with 1, following the SQLite
convention.
"""

from .split_text_field import _split_text_field


def _get_word(textfield, i):
    splitted = _split_text_field(textfield)
    if i > 0 and i - 1 < len(splitted):
        return splitted[i - 1]
    return None
