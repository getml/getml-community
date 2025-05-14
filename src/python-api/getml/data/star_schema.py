# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Special container for star schemata.
"""

from inspect import cleandoc
from textwrap import indent
from typing import Dict, Optional, Union

from getml.data.columns import StringColumnView
from getml.data.placeholder import OnType, TimeStampsType

from .columns import FloatColumn, StringColumn
from .container import Container
from .data_frame import DataFrame
from .data_model import DataModel
from .relationship import many_to_many
from .view import View


class StarSchema:
    """
    A StarSchema is a simplifying abstraction that can be used
    for machine learning problems that can be organized in a simple
    [star schema](https://en.wikipedia.org/wiki/Star_schema).

    It unifies [`Container`][getml.data.Container] and
    [`DataModel`][getml.data.DataModel] thus abstracting away the need to
    differentiate between the concrete data and the abstract data model.

    The class is designed using
    [composition ](https://en.wikipedia.org/wiki/Composition_over_inheritance)
    - it *is* neither [`Container`][getml.data.Container] nor [`DataModel`][getml.data.DataModel],
    but *has* both of them.

    This means that you can always fall back to the more flexible methods using
    [`Container`][getml.data.Container] and [`DataModel`][getml.data.DataModel] by directly
    accessing the attributes `container` and `data_model`.

    Attributes:
        population:
            The population table defines the
            [statistical population ](https://en.wikipedia.org/wiki/Statistical_population)
            of the machine learning problem and contains the target variables.

        alias:
            The alias to be used for the population table. This is required,
            if *population* is a [`View`][getml.data.View].

        peripheral:
            The peripheral tables are joined onto *population* or other
            peripheral tables. Note that you can also pass them using
            [`join`][getml.data.StarSchema.join].

        split:
            Contains information on how you want to split *population* into
            different [`Subset`][getml.data.Subset] s.
            Also refer to [`split`][getml.data.split].

        deep_copy:
            Whether you want to create deep copies or your tables.

        train:
            The population table used in the *train*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        validation:
            The population table used in the *validation*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        test:
            The population table used in the *test*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        kwargs:
            The population table used in [`Subset`][getml.data.Subset] s
            other than the predefined *train*, *validation* and *test* subsets.
            You can call these subsets anything you want to and can access them
            just like *train*, *validation* and *test*.
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

            ??? example
                ```python
                # Pass the subset.
                star_schema = getml.data.StarSchema(
                    my_subset=my_data_frame)

                # You can access the subset just like train,
                # validation or test
                my_pipeline.fit(star_schema.my_subset)
                ```

    ??? example
        Note that this example is taken from the
        [loans notebook](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/loans.ipynb).

        You might also want to refer to
        [`DataFrame`][getml.DataFrame], [`View`][getml.data.View]
        and [`Pipeline`][getml.Pipeline].

        ```python
        # First, we insert our data.
        # population_train and population_test are either
        # DataFrames or Views. The population table
        # defines the statistical population of your
        # machine learning problem and contains the
        # target variables.
        star_schema = getml.data.StarSchema(
            train=population_train,
            test=population_test
        )

        # meta, order and trans are either
        # DataFrames or Views.
        # Because this is a star schema,
        # all joins take place on the population
        # table.
        star_schema.join(
            trans,
            on="account_id",
            time_stamps=("date_loan", "date")
        )

        star_schema.join(
            order,
            on="account_id",
        )

        star_schema.join(
            meta,
            on="account_id",
        )

        # Now you can insert your data model,
        # your preprocessors, feature learners,
        # feature selectors and predictors
        # into the pipeline.
        # Note that the pipeline only knows
        # the abstract data model, but hasn't
        # seen the actual data yet.
        pipe = getml.Pipeline(
            data_model=star_schema.data_model,
            preprocessors=[mapping],
            feature_learners=[fast_prop],
            feature_selectors=[feature_selector],
            predictors=predictor,
        )

        # Now, we pass the actual data.
        # This passes 'population_train' and the
        # peripheral tables (meta, order and trans)
        # to the pipeline.
        pipe.check(star_schema.train)

        pipe.fit(star_schema.train)

        pipe.score(star_schema.test)

        # To generate predictions on new data,
        # it is sufficient to use a Container.
        # You don't have to recreate the entire
        # StarSchema, because the abstract data model
        # is stored in the pipeline.
        container = getml.data.Container(
            population=population_new)

        container.add(
            trans=trans_new,
            order=order_new,
            meta=meta_new)

        predictions = pipe.predict(container.full)
        ```
        If you don't already have a train and test set,
        you can use a function from the
        [`split`][getml.data.split] module.

        ```python
        split = getml.data.split.random(
            train=0.8, test=0.2)

        star_schema = getml.data.StarSchema(
            population=population_all,
            split=split,
        )

        # The remaining code is the same as in
        # the example above. In particular,
        # star_schema.train and star_schema.test
        # work just like above.
        ```
    """

    def __init__(
        self,
        population: Optional[Union[DataFrame, View]] = None,
        alias: Optional[str] = None,
        peripheral: Optional[Dict[str, Union[DataFrame, View]]] = None,
        split: Optional[Union[StringColumn, StringColumnView]] = None,
        deep_copy: Optional[bool] = False,
        train: Optional[Union[DataFrame, View]] = None,
        validation: Optional[Union[DataFrame, View]] = None,
        test: Optional[Union[DataFrame, View]] = None,
        **kwargs: Optional[Union[DataFrame, View]],
    ):
        if (population is None or isinstance(population, View)) and alias is None:
            raise ValueError(
                "If 'population' is None or a getml.data.View, you must set an alias."
            )

        self._alias = alias or population.name

        self._container = Container(
            population=population,
            peripheral=peripheral,
            split=split,
            deep_copy=deep_copy,
            train=train,
            validation=validation,
            test=test,
            **kwargs,
        )

        def get_placeholder():
            if population is not None:
                return population.to_placeholder(alias)
            if train is not None:
                return train.to_placeholder(alias)
            if validation is not None:
                return validation.to_placeholder(alias)
            if test is not None:
                return test.to_placeholder(alias)
            assert (
                len(kwargs) > 0
            ), "This should have been checked by Container.__init__."
            return kwargs[list(kwargs.keys())[0]].to_placeholder(alias)

        self._data_model = DataModel(get_placeholder())

    def __dir__(self):
        attrs = dir(type(self)) + [key[1:] for key in list(vars(self))]
        attrs += dir(self.container)
        attrs += dir(self.data_model)
        return list(set(attrs))

    def __iter__(self):
        yield from [self.population] + list(self.peripheral.values())

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            return super().__getattribute__(key)

    def __getitem__(self, key):
        attrs = vars(self)

        if key in attrs:
            return attrs[key]

        if "_" + key in attrs:
            return attrs["_" + key]

        try:
            return attrs["_container"][key]
        except KeyError:
            return attrs["_data_model"][key]

    def __repr__(self):
        template = cleandoc(
            """
            data model

            {data_model}


            container

            {container}
            """
        )
        return template.format(
            data_model=indent(repr(self.data_model), "  "),
            container=indent(repr(self.container), "  "),
        )

    def _repr_html_(self):
        template = cleandoc(
            """
            <span style='font-size: 1.2rem; font-weight: 500;'>data model</span>
            {data_model}
            <span style='font-size: 1.2rem; font-weight: 500;'>container</span>
            {container}
            """
        )
        return template.format(
            data_model=self.data_model._repr_html_(),
            container=self.container._repr_html_(),
        )

    @property
    def container(self) -> Container:
        """
        The underlying [`Container`][getml.data.Container].

        Returns:
            The underlying container.
        """
        return self._container

    @property
    def data_model(self) -> DataModel:
        """
        The underlying [`DataModel`][getml.data.DataModel].

        Returns:
            The underlying data model.
        """
        return self._data_model

    def join(
        self,
        right_df: Union[DataFrame, View],
        alias: Optional[str] = None,
        on: OnType = None,
        time_stamps: TimeStampsType = None,
        relationship: str = many_to_many,
        memory: Optional[float] = None,
        horizon: Optional[float] = None,
        lagged_targets: bool = False,
        upper_time_stamp: Optional[str] = None,
    ):
        """
        Joins a [`DataFrame`][getml.DataFrame] or [`View`][getml.data.View]
        to the population table.

        In a [`StarSchema`][getml.data.StarSchema] or [`TimeSeries`][getml.data.TimeSeries],
        all joins take place on the population table. If you want to create more
        complex data models, use [`DataModel`][getml.data.DataModel] instead.

        ??? example
            This example will construct a data model in which the
            'population_table' depends on the 'peripheral_table' via
            the 'join_key' column. In addition, only those rows in
            'peripheral_table' for which 'time_stamp' is smaller or
            equal to the 'time_stamp' in 'population_table' are considered:

            ```python
            star_schema = getml.data.StarSchema(
                population=population_table, split=split)

            star_schema.join(
                peripheral_table,
                on="join_key",
                time_stamps="time_stamp"
            )
            ```

            If the relationship between two tables is many-to-one or one-to-one
            you should clearly say so:
            ```python
            star_schema.join(
                peripheral_table,
                on="join_key",
                time_stamps="time_stamp",
                relationship=getml.data.relationship.many_to_one,
            )
            ```
            Please also refer to [`relationship`][getml.data.relationship].

            If the join keys or time stamps are named differently in the two
            different tables, use a tuple:

            ```python
            star_schema.join(
                peripheral_table,
                on=("join_key", "other_join_key"),
                time_stamps=("time_stamp", "other_time_stamp"),
            )
            ```

            You can join over more than one join key:

            ```python
            star_schema.join(
                peripheral_table,
                on=["join_key1", "join_key2", ("join_key3", "other_join_key3")],
                time_stamps="time_stamp",
                )
            ```

            You can also limit the scope of your joins using *memory*. This
            can significantly speed up training time. For instance, if you
            only want to consider data from the last seven days, you could
            do something like this:

            ```python
            star_schema.join(
                peripheral_table,
                on="join_key",
                time_stamps="time_stamp",
                memory=getml.data.time.days(7),
            )
            ```

            In some use cases, particularly those involving time series, it
            might be a good idea to use targets from the past. You can activate
            this using *lagged_targets*. But if you do that, you must
            also define a prediction *horizon*. For instance, if you want to
            predict data for the next hour, using data from the last seven days,
            you could do this:

            ```python
            star_schema.join(
                peripheral_table,
                on="join_key",
                time_stamps="time_stamp",
                lagged_targets=True,
                horizon=getml.data.time.hours(1),
                memory=getml.data.time.days(7),
            )
            ```

            Please also refer to [`time`][getml.data.time].

            If the join involves many matches, it might be a good idea to set the
            relationship to [`propositionalization`][getml.data.relationship.propositionalization].
            This forces the pipeline to always use a propositionalization
            algorithm for this join, which can significantly speed things up.

            ```python
            star_schema.join(
                peripheral_table,
                on="join_key",
                time_stamps="time_stamp",
                relationship=getml.data.relationship.propositionalization,
            )
            ```

            Please also refer to [`relationship`][getml.data.relationship].

        Args:
            right_df:
                The data frame or view you would like to join.

            alias:
                The name as which you want *right_df* to be referred to in
                the generated SQL code.

            on:
                The join keys to use. If none is passed, then everything
                will be joined to everything else.

            time_stamps:
                The time stamps used to limit the join.

            relationship:
                The relationship between the two tables. Must be from
                [`relationship`][getml.data.relationship].

            memory:
                The difference between the time stamps until data is 'forgotten'.
                Limiting your joins using memory can significantly speed up
                training time. Also refer to [`time`][getml.data.time].

            horizon:
                The prediction horizon to apply to this join.
                Also refer to [`time`][getml.data.time].

            lagged_targets:
                Whether you want to allow lagged targets. If this is set to True,
                you must also pass a positive, non-zero *horizon*.

            upper_time_stamp:
                Name of a time stamp in *right_df* that serves as an upper limit
                on the join.
        """

        if not isinstance(right_df, (DataFrame, View)):
            raise TypeError(
                f"Expected a {DataFrame} as 'right_df', got: {type(right_df)}."
            )

        if isinstance(right_df, View):
            if alias is None:
                raise ValueError(
                    "Setting an 'alias' is required if a getml.data.View is supplied "
                    "as a peripheral table."
                )

        def modify_join_keys(on):
            if isinstance(on, list):
                return [modify_join_keys(jk) for jk in on]

            if isinstance(on, (str, StringColumn)):
                on = (on, on)

            if on is not None and on:
                on = tuple(
                    jkey.name if isinstance(jkey, StringColumn) else jkey for jkey in on
                )

            return on

        def modify_time_stamps(time_stamps):
            if isinstance(time_stamps, (str, FloatColumn)):
                time_stamps = (time_stamps, time_stamps)

            if time_stamps is not None:
                time_stamps = tuple(
                    time_stamp.name
                    if isinstance(time_stamp, FloatColumn)
                    else time_stamp
                    for time_stamp in time_stamps
                )

            return time_stamps

        on = modify_join_keys(on)

        time_stamps = modify_time_stamps(time_stamps)

        upper_time_stamp = (
            upper_time_stamp.name
            if isinstance(upper_time_stamp, FloatColumn)
            else upper_time_stamp
        )

        right = right_df.to_placeholder(alias)

        self.data_model.population.join(
            right=right,
            on=on,
            time_stamps=time_stamps,
            relationship=relationship,
            memory=memory,
            horizon=horizon,
            lagged_targets=lagged_targets,
            upper_time_stamp=upper_time_stamp,
        )

        alias = alias or right_df.name

        self.container.add(**{alias: right_df})

    def sync(self):
        """
        Synchronizes the last change with the data to avoid warnings that the data
        has been changed.

        This is only a problem when `deep_copy=False`.
        """
        self.container.sync()
