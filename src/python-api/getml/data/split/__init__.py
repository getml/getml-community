# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Splits data into a training, testing, validation or other sets.
"""

from .concat import concat
from .random import random
from .time import time

__all__ = ("concat", "random", "time")
