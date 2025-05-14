# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Format the column"""

from getml.utilities.formatting import _ColumnFormatter


def _format(self):
    formatted = _ColumnFormatter(self)
    return formatted
