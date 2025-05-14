# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Checks whether a textfield contains a certain keyword.
"""

from .split_text_field import _split_text_field


def _contains(textfield, keyword):
    splitted = _split_text_field(textfield)
    k = keyword.lower()
    return len(["" for s in splitted if s == k])
