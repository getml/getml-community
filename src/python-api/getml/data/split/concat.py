# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Concatenates data.
"""

from typing import Tuple

from getml.data.columns import StringColumnView
from getml.data.columns.columns import rowid
from getml.data.columns.from_value import from_value
from getml.data.concat import concat as _concat
from getml.data.data_frame import DataFrame
from getml.data.helpers import _is_non_empty_typed_list
from getml.data.view import View


def concat(name: str, **kwargs: DataFrame) -> Tuple[DataFrame, StringColumnView]:
    """
    Concatenates several data frames into and produces a split
    column that keeps track of their origin.

    Args:
        name:
            The name of the data frame you would like to create.

        kwargs:
            The data frames you would like
            to concat with the name in which they should appear
            in the split column.

    Returns:
        A tuple containing the concatenated data frame and the split column.

    ??? example
        A common use case for this functionality are [`TimeSeries`][getml.data.TimeSeries]:
        ```python
        data_train = getml.DataFrame.from_pandas(
            datatraining_pandas, name='data_train')

        data_validate = getml.DataFrame.from_pandas(
            datatest_pandas, name='data_validate')

        data_test = getml.DataFrame.from_pandas(
            datatest2_pandas, name='data_test')

        population, split = getml.data.split.concat(
            "population", train=data_train, validate=data_validate, test=data_test)

        ...

        time_series = getml.data.TimeSeries(
            population=population, split=split)

        my_pipeline.fit(time_series.train)
        ```
    """

    if not _is_non_empty_typed_list(list(kwargs.values()), [DataFrame, View]):
        raise ValueError(
            "'kwargs' must be non-empty and contain getml.DataFrames "
            + "or getml.data.Views."
        )

    names = list(kwargs.keys())

    first = kwargs[names[0]]

    population = first.copy(name) if isinstance(first, DataFrame) else first.to_df(name)

    split = from_value(names[0])

    assert isinstance(split, StringColumnView), "Should be a StringColumnView"

    for new_df_name in names[1:]:
        split = split.update(rowid() > population.nrows(), new_df_name)  # type: ignore
        population = _concat(name, [population, kwargs[new_df_name]])

    return population, split[: population.nrows()]  # type: ignore
