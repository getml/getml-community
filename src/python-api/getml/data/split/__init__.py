# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Helps you split data into a training, testing, validation or other sets.

Examples:
    Split at random:

    .. code-block:: python

        split = getml.data.split.random(
            train=0.8, test=0.1, validation=0.1
        )

        train_set = data_frame[split=='train']
        validation_set = data_frame[split=='validation']
        test_set = data_frame[split=='test']

    Split over time:

    .. code-block:: python

        validation_begin = getml.data.time.datetime(2010, 1, 1)
        test_begin = getml.data.time.datetime(2011, 1, 1)

        split = getml.data.split.time(
            population=data_frame,
            time_stamp="ds",
            test=test_begin,
            validation=validation_begin
        )

        # Contains all data before 2010-01-01 (not included)
        train_set = data_frame[split=='train']

        # Contains all data between 2010-01-01 (included) and 2011-01-01 (not included)
        validation_set = data_frame[split=='validation']

        # Contains all data after 2011-01-01 (included)
        test_set = data_frame[split=='test']
"""

from .concat import concat
from .random import random
from .time import time

__all__ = ("concat", "random", "time")
