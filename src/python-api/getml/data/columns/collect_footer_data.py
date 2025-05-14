# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Collects the data necessary for displaying the column footer."""

from typing import NamedTuple, Optional, Union

from .constants import _views


class Footer(NamedTuple):
    """
    Contains the data to be shown
    in the footer of the data frame or
    column.
    """

    n_rows: Union[int, str]
    type: str
    data_frame: Optional[str] = None
    url: Optional[str] = None


def _collect_footer_data(self) -> Footer:
    if type(self).__name__ in _views:
        nrows_is_known = not isinstance(self.length, str)

        return Footer(
            n_rows=self.length if nrows_is_known else self.length + " number of ",
            type=type(self).__name__,
        )

    return Footer(
        n_rows=len(self),
        data_frame=self.cmd["df_name_"] if "df_name_" in self.cmd else "",
        type=type(self).__name__,
        url=self._monitor_url,
    )
