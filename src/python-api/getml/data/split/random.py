# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Splits data at random.
"""

import numbers

import numpy as np

from getml.data.columns import StringColumnView
from getml.data.columns import random as random_col
from getml.data.columns.from_value import from_value
from getml.data.helpers import _is_typed_list


def random(
    seed: int = 5849,
    train: float = 0.8,
    test: float = 0.2,
    validation: float = 0,
    **kwargs: float,
) -> StringColumnView:
    """
    Returns a [`StringColumnView`][getml.data.columns.StringColumnView] that
    can be used to randomly divide data into training, testing,
    validation or other sets.

    Args:
        seed:
            Seed used for the random number generator.

        train:
            The share of random samples assigned to
            the training set.

        validation:
            The share of random samples assigned to
            the validation set.

        test:
            The share of random samples assigned to
            the test set.

        kwargs:
            Any other sets you would like to assign.
            You can name these sets whatever you want to (in our example,
            we called it 'other').

    ??? example
        ```python
        split = getml.data.split.random(
            train=0.8, test=0.1, validation=0.05, other=0.05
        )

        train_set = data_frame[split=='train']
        validation_set = data_frame[split=='validation']
        test_set = data_frame[split=='test']
        other_set = data_frame[split=='other']
        ```

    """

    values = np.asarray([train, validation, test] + list(kwargs.values()))

    if not _is_typed_list(values.tolist(), numbers.Real):
        raise ValueError("All values must be real numbers.")

    if np.abs(np.sum(values) - 1.0) > 0.0001:
        raise ValueError(
            "'train', 'validation', 'test' and all other sets must add up to 1, "
            + "but add up to "
            + str(np.sum(values))
            + "."
        )

    upper_bounds = np.cumsum(values)
    lower_bounds = upper_bounds - values

    names = ["train", "validation", "test"] + list(kwargs.keys())

    col: StringColumnView = from_value("train")  # type: ignore

    assert isinstance(col, StringColumnView), "Should be a StringColumnView"

    for i in range(len(names)):
        col = col.update(  # type: ignore
            (random_col(seed=seed) >= lower_bounds[i])  # type: ignore
            & (random_col(seed=seed) < upper_bounds[i]),
            names[i],
        )

    return col
