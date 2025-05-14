# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
HTML representation of the column.
"""


def _repr_html(self):
    formatted = self._format()
    footer = self._collect_footer_data()
    return formatted._render_html(footer=footer)
