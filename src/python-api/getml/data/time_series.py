# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Special container for time series - abstracts away self-joins.
"""

from typing import Dict, Optional, Union

from getml.data.columns import StringColumn, StringColumnView
from getml.data.helpers import OnType

from .columns import FloatColumn, FloatColumnView
from .data_frame import DataFrame
from .helpers import _finditems
from .roles import time_stamp
from .star_schema import StarSchema
from .view import View


class TimeSeries(StarSchema):
    """
    A TimeSeries is a simplifying abstraction that can be used
    for machine learning problems on time series data.

    It unifies [`Container`][getml.data.Container] and
    [`DataModel`][getml.data.DataModel] thus abstracting away the need to
    differentiate between the concrete data and the abstract data model.
    It also abstracts away the need for
    [self joins](https://en.wikipedia.org/wiki/Join_(SQL)#Self-join).

    Attributes:
        time_stamps:
            The time stamps used to limit the self-join.

        population:
            The population table defines the
            [statistical population ](https://en.wikipedia.org/wiki/Statistical_population)
            of the machine learning problem and contains the target variables.

        alias:
            The alias to be used for the population table. If it isn't set, the 'population'
            will be used as the alias. To explicitly set an alias for the
            peripheral table, use [`with_name`][getml.DataFrame.with_name].

        peripheral:
            The peripheral tables are joined onto *population* or other
            peripheral tables. Note that you can also pass them using
            [`add`][getml.data.Container.add].

        split:
            Contains information on how you want to split *population* into
            different [`Subset`][getml.data.Subset] s.
            Also refer to [`split`][getml.data.split].

        deep_copy:
            Whether you want to create deep copies or your tables.

        on:
            The join keys to use. If none is passed, then everything
            will be joined to everything else.

        memory:
            The difference between the time stamps until data is 'forgotten'.
            Limiting your joins using memory can significantly speed up
            training time. Provide the value in seconds, alternatively use
             the convenience functions from [`time`][getml.data.time].

        horizon:
            The prediction horizon to apply to this join.
            Provide the value in seconds, alternatively use
            the convenience functions from [`time`][getml.data.time].

        lagged_targets:
            Whether you want to allow lagged targets. If this is set to True,
            you must also pass a positive, non-zero *horizon*.

        upper_time_stamp:
            Name of a time stamp in *right_df* that serves as an upper limit
            on the join.

    ??? example
        ```python
        # All rows before row 10500 will be used for training.
        split = getml.data.split.time(data_all, "rowid", test=10500)

        time_series = getml.data.TimeSeries(
            population=data_all,
            time_stamps="rowid",
            split=split,
            lagged_targets=False,
            memory=30,
        )

        pipe = getml.Pipeline(
            data_model=time_series.data_model,
            feature_learners=[...],
            predictors=...
        )

        pipe.check(time_series.train)

        pipe.fit(time_series.train)

        pipe.score(time_series.test)

        # To generate predictions on new data,
        # it is sufficient to use a Container.
        # You don't have to recreate the entire
        # TimeSeries, because the abstract data model
        # is stored in the pipeline.
        container = getml.data.Container(
            population=population_new,
        )

        # Add the data as a peripheral table, for the
        # self-join.
        container.add(population=population_new)

        predictions = pipe.predict(container.full)
        ```
    """

    def __init__(
        self,
        population: Union[DataFrame, View],
        time_stamps: str,
        alias: Optional[str] = None,
        peripheral: Optional[Dict[str, Union[DataFrame, View]]] = None,
        split: Optional[Union[StringColumn, StringColumnView]] = None,
        deep_copy: Optional[bool] = False,
        on: OnType = None,
        memory: Optional[float] = None,
        horizon: Optional[float] = None,
        lagged_targets: bool = False,
        upper_time_stamp: Optional[str] = None,
    ):
        if not isinstance(population, (DataFrame, View)):
            raise TypeError(
                "'population' must be a getml.DataFrame or a getml.data.View"
            )

        if isinstance(time_stamps, FloatColumn):
            time_stamps = time_stamps.name

        if isinstance(time_stamps, FloatColumnView):
            if "rowid" in _finditems("operator_", time_stamps.cmd):
                time_stamps = "rowid"

        population = (
            population.with_column(
                population.rowid, name="rowid", role=time_stamp
            ).with_unit(names="rowid", unit="rowid", comparison_only=True)
            if time_stamps == "rowid"
            else population
        )

        alias = "population" if alias is None else alias

        super().__init__(
            population=population,
            alias=alias,
            peripheral=peripheral,
            split=split,
            deep_copy=deep_copy,
        )

        self.on = on
        self.time_stamps = time_stamps
        self.memory = memory
        self.horizon = horizon
        self.lagged_targets = lagged_targets
        self.upper_time_stamp = upper_time_stamp

        if not isinstance(on, list):
            on = [on]

        for o in on:
            self._add_joins(o)

    def _add_joins(self, on):
        self.join(
            right_df=self.population,
            alias=self.population.name,
            on=self.on,
            time_stamps=self.time_stamps,
            memory=self.memory,
            horizon=self.horizon,
            lagged_targets=self.lagged_targets,
            upper_time_stamp=self.upper_time_stamp,
        )
