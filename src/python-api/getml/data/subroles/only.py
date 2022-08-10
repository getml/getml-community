# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Columns marked with a subrole in this submodule will only be used
for the specified purpose and nothing else.

Example:
    .. code-block:: python

        # Only the EmailDomain preprocessor will be applied
        # to "emails". All other preprocessors, feature learners,
        # feature selectors and predictors will ignore this column.
        my_data_frame.set_subroles("emails", getml.data.subroles.only.email)
"""

email = "only email"
"""
A column with this subrole will only be
used for the :class:`~getml.preprocessors.EmailDomain`
preprocessor and nothing else. It will be ignored by all other preprocessors,
feature learners and predictors.
"""

substring = "only substring"
"""
A column with this subrole will only be
used for the :class:`~getml.preprocessors.Substring`
preprocessor and nothing else. It will be ignored by all other preprocessors,
feature learners and predictors.
"""

__all__ = ("email", "substring")

_all_only = [email, substring]
