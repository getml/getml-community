# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


from typing import Final

from getml.data.subroles.types import OnlyEmail, OnlySubstring

"""
Columns marked with a subrole in this submodule will only be used
for the specified purpose and nothing else.

??? example
    ```python
    # Only the EmailDomain preprocessor will be applied
    # to "emails". All other preprocessors, feature learners,
    # feature selectors and predictors will ignore this column.
    my_data_frame.set_subroles("emails", getml.data.subroles.only.email)
    ```

"""

email: Final[OnlyEmail] = "only email"
"""
A column with this subrole will only be
used for the [`EmailDomain`][getml.preprocessors.EmailDomain]
preprocessor and nothing else. It will be ignored by all other preprocessors,
feature learners and predictors.
"""

substring: Final[OnlySubstring] = "only substring"
"""
A column with this subrole will only be
used for the [`Substring`][getml.preprocessors.Substring]
preprocessor and nothing else. It will be ignored by all other preprocessors,
feature learners and predictors.
"""

__all__ = ("email", "substring")
