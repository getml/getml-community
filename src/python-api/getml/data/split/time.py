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
from typing import Dict, Optional, Union

import numpy as np

from getml.data.columns import FloatColumn, FloatColumnView, StringColumnView
from getml.data.columns.from_value import from_value
from getml.data.data_frame import DataFrame
from getml.data.helpers import _is_typed_list
from getml.data.view import View


def time(
    population: DataFrame,
    time_stamp: Union[str, FloatColumn, FloatColumnView],
    validation: Optional[Union[float, int, np.datetime64]] = None,
    test: Optional[Union[float, int, np.datetime64]] = None,
    **kwargs: Union[float, int, np.datetime64],
) -> StringColumnView:
    """
    Returns a [`StringColumnView`][getml.data.columns.StringColumnView] that can be used to divide
    data into training, testing, validation or other sets.

    The arguments are
    `key=value` pairs of names (`key`) and starting points (`value`).
    The starting point defines the left endpoint of the subset. Intervals are left
    closed and right open, such that $[value, next value)$.  The (unnamed) subset
    left from the first named starting point, i.e.  $[0, first value)$, is always
    considered to be the training set.

    Args:
        population:
            The population table you would like to split.

        time_stamp:
            The name of the time stamp column in the population table
            you want to use. Ideally, the role of said column would be
            [`time_stamp`][getml.data.roles.time_stamp]. If you want to split on the rowid,
            then pass "rowid" to `time_stamp`.

        validation:
            The start date of the validation set.

        test:
            The start date of the test set.

        kwargs:
            Any other sets you would like to assign.
            You can name these sets whatever you want to (in our example,
            we called it 'other').

    ??? example
        ```python
        validation_begin = getml.data.time.datetime(2010, 1, 1)
        test_begin = getml.data.time.datetime(2011, 1, 1)
        other_begin = getml.data.time.datetime(2012, 1, 1)

        split = getml.data.split.time(
            population=data_frame,
            time_stamp="ds",
            test=test_begin,
            validation=validation_begin,
            other=other_begin
        )

        # Contains all data before 2010-01-01 (not included)
        train_set = data_frame[split=='train']

        # Contains all data between 2010-01-01 (included) and 2011-01-01 (not included)
        validation_set = data_frame[split=='validation']

        # Contains all data between 2011-01-01 (included) and 2012-01-01 (not included)
        test_set = data_frame[split=='test']

        # Contains all data after 2012-01-01 (included)
        other_set = data_frame[split=='other']
        ```
    """
    if not isinstance(population, (DataFrame, View)):
        raise ValueError("'population' must be a DataFrame or a View.")

    if not isinstance(time_stamp, (str, FloatColumn, FloatColumnView)):
        raise ValueError(
            "'time_stamp' must be a string, a FloatColumn, or a FloatColumnView."
        )

    if not test and not validation and not kwargs:
        raise ValueError("You have to supply at least one starting point.")

    defaults: Dict[str, Optional[Union[float, int, np.datetime64]]] = {
        "test": test,
        "validation": validation,
    }

    sets = {name: value for name, value in defaults.items() if value is not None}

    sets.update({**kwargs})

    values = np.asarray(list(sets.values()))
    index = np.argsort(values)
    values = values[index]

    if not _is_typed_list(values.tolist(), numbers.Real):
        raise ValueError("All values must be real numbers.")

    names = np.asarray(list(sets.keys()))
    names = names[index]

    if isinstance(time_stamp, str):
        time_stamp_col = (
            population[time_stamp] if time_stamp != "rowid" else population.rowid
        )
    else:
        time_stamp_col = time_stamp

    col: StringColumnView = from_value("train")  # type: ignore

    assert isinstance(col, StringColumnView), "Should be a StringColumnView"

    for i in range(len(names)):
        col = col.update(  # type: ignore
            time_stamp_col >= values[i],
            names[i],
        )

    return col
