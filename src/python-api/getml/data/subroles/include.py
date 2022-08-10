# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Columns marked with a subrole in this submodule will be used for the
specified purpose without excluding other purposes.

Example:
    .. code-block:: python

        # The Substring preprocessor will be applied to this column.
        # But other preprocessors, feature learners or predictors
        # are not excluded from using it as well.
        my_data_frame.set_subroles(
            "ucc", getml.data.subroles.include.substring)
"""

email = "include email"
"""
A column with this subrole will be
used for the :class:`~getml.preprocessors.EmailDomain`
preprocessor.
"""

substring = "include substring"
"""
A column with this subrole will be
used for the :class:`~getml.preprocessors.Substring`
preprocessor.
"""

__all__ = ("email", "substring")

_all_include = [email, substring]
