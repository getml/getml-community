# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


def _get_scalar(column, index) -> float:
    index = index if index >= 0 else len(column) + index
    return column[index : index + 1].to_numpy()[0]
