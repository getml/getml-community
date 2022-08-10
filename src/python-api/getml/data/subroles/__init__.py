# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Subroles allow for more fine-granular control of how certain columns
will be used by the pipeline.

A column can have no subrole, one subrole or several subroles.

Example:
    .. code-block:: python

        # The Relboost feature learning algorithm will
        # ignore this column.
        my_data_frame.set_subroles(
            "my_column", getml.data.subroles.exclude.relboost)

        # The Substring preprocessor will be applied to this column.
        # But other preprocessors, feature learners or predictors
        # are not excluded from using it as well.
        my_data_frame.set_subroles(
            "ucc", getml.data.subroles.include.substring)

        # Only the EmailDomain preprocessor will be applied
        # to "emails". All other preprocessors, feature learners,
        # feature selectors and predictors will ignore this column.
        my_data_frame.set_subroles("emails", getml.data.subroles.only.email)
"""

from .exclude import _all_exclude
from .include import _all_include
from .only import _all_only

_all_subroles = _all_exclude + _all_include + _all_only

__all__ = ("exclude", "include", "only")
