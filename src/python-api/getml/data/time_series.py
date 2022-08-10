# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Special container for time series - abstracts away self-joins.
"""

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

    It unifies :class:`~getml.data.Container` and
    :class:`~getml.data.DataModel` thus abstracting away the need to
    differentiate between the concrete data and the abstract data model.
    It also abstracts away the need for
    `self joins <https://en.wikipedia.org/wiki/Join_(SQL)#Self-join>`_.

    Args:
        population (:class:`~getml.DataFrame` or :class:`~getml.data.View`):
            The population table defines the
            `statistical population <https://en.wikipedia.org/wiki/Statistical_population>`_
            of the machine learning problem and contains the target variables.

        time_stamps (str):
            The time stamps used to limit the self-join.

        alias (str, optional):
            The alias to be used for the population table. If it isn't set, the 'population'
            will be used as the alias. To explicitly set an alias for the
            peripheral table, use :meth:`~getml.DataFrame.with_name`.

        peripheral (dict, optional):
            The peripheral tables are joined onto *population* or other
            peripheral tables. Note that you can also pass them using
            :meth:`~getml.data.Container.add`.

        split (:class:`~getml.data.columns.StringColumn` or :class:`~getml.data.columns.StringColumnView`, optional):
            Contains information on how you want to split *population* into
            different :class:`~getml.data.Subset` s.
            Also refer to :mod:`~getml.data.split`.

        deep_copy (bool, optional):
            Whether you want to create deep copies or your tables.

        on (None, string, Tuple[str] or List[Union[str, Tuple[str]]], optional):
            The join keys to use. If none is passed, then everything
            will be joined to everything else.

        memory (float, optional):
            The difference between the time stamps until data is 'forgotten'.
            Limiting your joins using memory can significantly speed up
            training time. Also refer to :mod:`~getml.data.time`.

        horizon (float, optional):
            The prediction horizon to apply to this join.
            Also refer to :mod:`~getml.data.time`.

        lagged_targets (bool, optional):
            Whether you want to allow lagged targets. If this is set to True,
            you must also pass a positive, non-zero *horizon*.

        upper_time_stamp (str, optional):
            Name of a time stamp in *right_df* that serves as an upper limit
            on the join.

    Example:
        .. code-block:: python

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
    """

    def __init__(
        self,
        population,
        time_stamps,
        alias=None,
        alias2=None,
        peripheral=None,
        split=None,
        deep_copy=False,
        on=None,
        memory=None,
        horizon=None,
        lagged_targets=False,
        upper_time_stamp=None,
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
