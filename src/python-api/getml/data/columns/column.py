# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Base object not meant to be called directly."""

from typing import Any, Dict, Optional

import getml.communication as comm


class _Column:
    """
    Base object not meant to be called directly.
    """

    # -------------------------------------------------------------------------

    def __init__(self):
        self.cmd: Dict[str, Any] = {}

    # ------------------------------------------------------------

    @property
    def _monitor_url(self) -> Optional[str]:
        """
        The URL of the column.
        """
        url = comm._monitor_url()
        if not url:
            return None
        return (
            url
            + "getcolumn/"
            + comm._get_project_name()
            + "/"
            + self.cmd["df_name_"]
            + "/"
            + self.name
            + "/"
        )

    # ------------------------------------------------------------

    @property
    def name(self):
        """
        The name of this column.
        """
        return self.cmd["name_"]

    # -------------------------------------------------------------------------

    @property
    def role(self):
        """
        The role of this column.

        Roles are needed by the feature learning algorithm, so it knows how
        to treat the columns.
        """
        return self.cmd["role_"]
