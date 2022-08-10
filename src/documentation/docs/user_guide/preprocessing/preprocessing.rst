.. _preprocessing:

Preprocessing
=============

As preprocessing, we categorize operations on data frames, that are not directly
related to the relational data model. While feature learning and
propositionalization deal with *relational data structures* and result in a
single-table representation thereof, we categorize all operations that work on
*single tables* as preprocessing. This includes numerical transformations,
encoding techniques, or alternative representations.

getML's preprocessors allow you to extract domains from email addresses
(:class:`~getml.preprocessors.EmailDomain`), impute missing values
(:class:`~getml.preprocessors.Imputation`), map categorical columns to a
continuous  representation (:class:`~getml.preprocessors.Mapping`), extract
seasonal components from time stamps (:class:`~getml.preprocessors.Seasonal`),
extract sub strings from string-based columns
(:class:`~getml.preprocessors.Substring`) and split up
:const:`~getml.data.roles.text` columns
(:class:`~getml.preprocessors.TextFieldSplitter`). Preprocessing operations in
getML are very efficient and happen really fast. In fact, most of the time you
won't even notice the prescence of a preprocessor in your pipeline. getML's
preprocessors operate on an abstract level without polluting your original data,
are evaluated lazily and their set-up requires minimal effort.

Here is a small example that shows the :class:`~getml.preprocessors.Seasonal`
preprocessor in action.

.. code-block:: python

    >>> import getml

    >>> getml.project.switch("seasonal")

    >>> traffic = getml.datasets.load_interstate94()

    # traffic explicitly holds seasonal components (hour, day, month, ...)
    # extracted from column ds; we copy traffic and delete all those components
    >>> traffic2 = traffic.drop(["hour", "weekday", "day", "month", "year"])

    >>> start_test = getml.data.time.datetime(2018, 3, 14)

    >>> split = getml.data.split.time(
    ...     population=traffic,
    ...     test=start_test,
    ...     time_stamp="ds",
    ... )

    >>> time_series1 = getml.data.TimeSeries(
    ...     population=traffic1,
    ...     split=split,
    ...     time_stamps="ds",
    ...     horizon=getml.data.time.hours(1),
    ...     memory=getml.data.time.days(7),
    ...     lagged_targets=True,
    ... )

    >>> time_series2 = getml.data.TimeSeries(
    ...     population=traffic2,
    ...     split=split,
    ...     time_stamps="ds",
    ...     horizon=getml.data.time.hours(1),
    ...     memory=getml.data.time.days(7),
    ...     lagged_targets=True,
    ... )

    >>> fast_prop = getml.feature_learning.FastProp(
    ...     loss_function=getml.feature_learning.loss_function.SquareLoss)

    >>> pipe1 = getml.pipeline.Pipeline(
    ...     data_model=time_series1.data_model,
    ...     feature_learners=[fast_prop],
    ...     predictors=[getml.predictors.XGBoostRegressor()]
    ... )

    >>> pipe2 = getml.pipeline.Pipeline(
    ...     data_model=time_series2.data_model,
    ...     preprocessors=[getml.preprocessors.Seasonal()],
    ...     feature_learners=[fast_prop],
    ...     predictors=[getml.predictors.XGBoostRegressor()]
    ... )

    # pipe1 includes no preprocessor but receives the data frame with the components
    >>> pipe1.fit(time_series1.train)

    # pipe2 includes the preprocessor; receives data w/o components
    >>> pipe2.fit(time_series2.train)

    >>> month_based1 = pipe1.features.filter(lambda feat: "month" in feat.sql)
    >>> month_based2 = pipe2.features.filter(
    ...     lambda feat: "COUNT( DISTINCT t2.\"strftime('%m'" in feat.sql
    ... )

    >>> month_based1[1].sql
    DROP TABLE IF EXISTS "FEATURE_1_10";

    CREATE TABLE "FEATURE_1_10" AS
    SELECT COUNT( t2."month"  ) - COUNT( DISTINCT t2."month" ) AS "feature_1_10",
        t1.rowid AS "rownum"
    FROM "POPULATION__STAGING_TABLE_1" t1
    LEFT JOIN "POPULATION__STAGING_TABLE_2" t2
    ON 1 = 1
    WHERE t2."ds, '+1.000000 hours'" <= t1."ds"
    AND ( t2."ds, '+7.041667 days'" > t1."ds" OR t2."ds, '+7.041667 days'" IS NULL )
    GROUP BY t1.rowid;

    >>> month_based2[0].sql
    DROP TABLE IF EXISTS "FEATURE_1_5";

    CREATE TABLE "FEATURE_1_5" AS
    SELECT COUNT( t2."strftime('%m', ds )"  ) - COUNT( DISTINCT t2."strftime('%m', ds )" ) AS "feature_1_5",
        t1.rowid AS "rownum"
    FROM "POPULATION__STAGING_TABLE_1" t1
    LEFT JOIN "POPULATION__STAGING_TABLE_2" t2
    ON 1 = 1
    WHERE t2."ds, '+1.000000 hours'" <= t1."ds"
    AND ( t2."ds, '+7.041667 days'" > t1."ds" OR t2."ds, '+7.041667 days'" IS NULL )
    GROUP BY t1.rowid;

If you compare both of the features above, you will notice they are exactly
the same: :code:`COUNT - COUNT(DISTINCT)` on the month component conditional on the
time-based restrictions introduced through memory and horizon.

While most of getML's preprocessors are straightforward, two of them deserve a
more detailed introduction: :class:`~getml.preprocessors.Mapping` and
:class:`~getml.preprocessors.TextFieldSplitter`.

.. _mappings:

Mappings
++++++++
:class:`~getml.preprocessors.Mapping` s are an alternative representation 
for categorical columns, text columns
and (quasi-categorical) discrete-numerical columns. Each discrete value
(category) of a categorical column is mapped to a continuous spectrum by
calculating the average target value for the subset of all rows belonging
to the respective category. For columns from peripheral tables, the average
target values are propagated back by traversing the relational structure.

Mappings are a simple and interpretable alternative representation for
categorical data. By introducing a continuous representation, mappings allow
getML's feature learning algorithms to apply arbitrary aggregations to
categorical columns. Further, mappings enable huge gains in efficiency when
learning patterns from categorical data. You can control the extend mappings are
utilized by specifying the minimum number of matching rows required for
categories that constitutes a mapping through the ``min_freq`` parameter.

Here is an example mapping from the `CORA notebook
<https://nbviewer.getml.com/github/getml/getml-demo/blob/master/cora.ipynb>`_:

  .. code-block:: sql

     DROP TABLE IF EXISTS "CATEGORICAL_MAPPING_1_1_1";
     CREATE TABLE "CATEGORICAL_MAPPING_1_1_1"(key TEXT NOT NULL PRIMARY KEY, value NUMERIC);
     INSERT INTO "CATEGORICAL_MAPPING_1_1_1"(key, value)
     VALUES('Case_Based', 0.7109826589595376),
           ('Rule_Learning', 0.07368421052631578),
           ('Reinforcement_Learning', 0.0576923076923077),
           ('Theory', 0.0547945205479452),
           ('Genetic_Algorithms', 0.03157894736842105),
           ('Neural_Networks', 0.02088772845953003),
           ('Probabilistic_Methods', 0.01293103448275862);

Inspecting the actual values, it's highly likely, that this mapping stems from
a feature learned by a sub learner targeting the label "Case_Based". In addition
to the trivial case, we can see that the next closed neighboring category is the
"Rule_Learning" category, to which 7.3 % of the papers citing the target papers are
categorized.

.. _text_fields:

Handling of free form text
++++++++++++++++++++++++++

getML provides the role :const:`~getml.data.roles.text` to annotate free form
text fields within relational data structures. Learning from
:const:`~getml.data.roles.text` columns works as follows: First, for each of the
:const:`~getml.data.roles.text` columns, a vocabulary is build by taking into
account the feature learner's text mining specific hyperparameter
``vocab_size``. If a text field contains words that belong to the vocabulary,
getML deals with columns of role :const:`~getml.data.roles.text` through one of
two approaches: Text fields can either can be integrated into features by
learning conditions based on the mere presence (or absence) of certain words in
those text fields (the default) or they can be split into a relational
bag-of-words representation by means of the
:class:`~getml.preprocessors.TextFieldSplitter` preprocessor.  Opting for the
second approach is as easy as adding the
:class:`~getml.preprocessors.TextFieldSplitter` to the list of
:attr:`~getml.pipeline.Pipeline.preprocessors` on your
:class:`~getml.pipeline.Pipeline`. The resulting bag of words can be viewed as
another one-to-many relationship within our data model where each row holding a
text field is related to n peripheral rows (n is the number of words in the text
field).  Consider the following example, where the text field is split into a
relational bag of words.

.. table:: One row of a table with a text field

    ======  ===========================================
    rownum  text field
    ======  ===========================================
    52      The quick brown fox jumps over the lazy dog
    ======  ===========================================

.. table:: The (implict) peripheral table that results from splitting

    ======  =====
    rownum  words
    ======  =====
    52      the
    52      quick
    52      brown
    52      fox
    52      jumps
    52      over
    52      the
    52      lazy
    52      dog
    ======  =====


As text fields now present another relation, getML's feature learning algorithms
are able to learn structural logic from text fields' contents by applying
aggregations over the resulting bag of words itself (:code:`COUNT WHERE words IN
('quick', 'jumps')`). Further, by utilizing :ref:`mappings`, any aggregation
applicable to a (mapped) categorical column can be applied to bag-of-words
mappings as well.

Note that the splitting of text fields can be computational expensive. If
performance suffers too much, you may resort to the default behavior by removing
the :class:`~getml.preprocessors.TextFieldSplitter` from your
:class:`~getml.pipeline.Pipeline`.

