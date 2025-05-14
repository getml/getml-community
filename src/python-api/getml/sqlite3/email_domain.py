# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Extracts the email domain.
"""


def _email_domain(textfield):
    splitted = textfield.split("@")

    if len(splitted) != 2:
        return ""

    if "." not in splitted[1]:
        return ""

    return splitted[1]
