# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Counts the number of words in a textfield.
"""

from .split_text_field import _split_text_field


def _num_words(textfield):
    splitted = _split_text_field(textfield)
    return len(splitted)
