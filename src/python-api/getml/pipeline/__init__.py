# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Contains handlers for all steps involved in a data science project after
data preparation:

- Automated feature learning
- Automated feature selection
- Training and evaluation of machine learning (ML) algorithms
- Deployment of the fitted models

??? example
    We assume that you have already set up your
    preprocessors (refer to [`preprocessors`][getml.preprocessors]),
    your feature learners (refer to [`feature_learning`][getml.feature_learning])
    as well as your feature selectors and predictors
    (refer to [`predictors`][getml.predictors], which can be used
    for prediction and feature selection).

    You might also want to refer to
    [`DataFrame`][getml.DataFrame], [`View`][getml.data.View],
    [`DataModel`][getml.data.DataModel], [`Container`][getml.data.Container],
    [`Placeholder`][getml.data.Placeholder] and
    [`StarSchema`][getml.data.StarSchema].

    If you want to create features for a time series problem,
    the easiest way to do so is to use the [`TimeSeries`][getml.data.TimeSeries]
    abstraction.

    Note that this example is taken from the
    [robot notebook ](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/robot.ipynb).

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

??? example
    If your data can be organized in a simple star schema,
    you can use [`StarSchema`][getml.data.StarSchema].
    [`StarSchema`][getml.data.StarSchema] unifies
    [`Container`][getml.data.Container] and [`DataModel`][getml.data.DataModel]:
    Note that this example is taken from the
    [loans notebook ](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/loans.ipynb).

    ```python

    # First, we insert our data into a StarSchema.
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
    ```

??? example
    [`StarSchema`][getml.data.StarSchema] is simpler,
    but cannot be used for more complex data models.
    The general approach is to use
    [`Container`][getml.data.Container] and [`DataModel`][getml.data.DataModel]:

    ```python

    # First, we insert our data into a Container.
    # population_train and population_test are either
    # DataFrames or Views.
    container = getml.data.Container(
        train=population_train,
        test=population_test
    )

    # meta, order and trans are either
    # DataFrames or Views. They are given
    # aliases, so we can refer to them in the
    # DataModel.
    container.add(
        meta=meta,
        order=order,
        trans=trans
    )

    # Freezing makes the container immutable.
    # This is not required, but often a good idea.
    container.freeze()

    # The abstract data model is constructed
    # using the DataModel class. A data model
    # does not contain any actual data. It just
    # defines the abstract relational structure.
    dm = getml.data.DataModel(
        population_train.to_placeholder("population")
    )

    dm.add(getml.data.to_placeholder(
        meta=meta,
        order=order,
        trans=trans)
    )

    dm.population.join(
        dm.trans,
        on="account_id",
        time_stamps=("date_loan", "date")
    )

    dm.population.join(
        dm.order,
        on="account_id",
    )

    dm.population.join(
        dm.meta,
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
        data_model=dm,
        preprocessors=[mapping],
        feature_learners=[fast_prop],
        feature_selectors=[feature_selector],
        predictors=predictor,
    )

    # This passes 'population_train' and the
    # peripheral tables (meta, order and trans)
    # to the pipeline.
    pipe.check(container.train)

    pipe.fit(container.train)

    pipe.score(container.test)
    ```
    Technically, you don't actually have to use a
    [`Container`][getml.data.Container]. You might as well do this
    (in fact, a [`Container`][getml.data.Container] is just
    syntactic sugar for this approach):

    ```python

    pipe.check(
        population_train,
        {"meta": meta, "order": order, "trans": trans},
    )

    pipe.fit(
        population_train,
        {"meta": meta, "order": order, "trans": trans},
    )

    pipe.score(
        population_test,
        {"meta": meta, "order": order, "trans": trans},
    )
    ```
    Or you could even do this. The order of the peripheral tables
    can be inferred from the `__repr__()` method of the pipeline,
    and it is usually in alphabetical order.

    ```python

    pipe.check(
        population_train,
        [meta, order, trans],
    )

    pipe.fit(
        population_train,
        [meta, order, trans],
    )

    pipe.score(
        population_test,
        [meta, order, trans],
    )
    ```
"""

from . import dialect, metrics
from .columns import Columns
from .features import Features
from .helpers2 import delete, exists, list_pipelines, load
from .pipeline import Pipeline
from .plots import Plots
from .scores_container import Scores
from .sql_code import SQLCode
from .tables import Tables

__all__ = (
    "delete",
    "dialect",
    "exists",
    "list_pipelines",
    "load",
    "metrics",
    "Columns",
    "Tables",
    "Features",
    "Plots",
    "Pipeline",
    "Scores",
    "SQLCode",
)
