.. _feature_engineering:

Feature engineering
===================

The deep learning revolution has enabled automated feature engineering for
images and sound data. Yet, for relational data and classical time series
analysis, feature engineering is still done by hand or using very simple brute
force methods. Our mission is to change that.

The automation of feature engineering on relational data and time
series is at the heart of the getML software suite. There are other
libraries that implement feature engineering tools on top of
frameworks like `data.tables` in R, `pandas` in Python, or `Apache
Spark`. In essence, they all use a brute force approach: Generate a
large number of features, then use some sort of feature selection
routine to pick a small subselection of them.

getML has chosen another path: Our highly efficient feature
learning algorithms produce features that are far more advanced
than what manual feature engineering could achieve or what could be
accomplished using simple brute force approaches.

.. _feature_engineering_definition:

Definition
----------

Feature engineering is the process of constructing variables, so-called
features, from a dataset. These features are used as the input for machine learning
algorithms. In most real-world datasets, the raw data is spread over multiple
tables and the task is to bring these tables together and construct features
based on their relationships. These features are stored in a flat feature table.
In other words, feature engineering is the operation of merging and aggregating
a relational data model into a flat (feature) table. From an academic point of view,
most machine learning algorithms used nowadays can be classified as *propositonal
learners.* The process of creating flat attribute-value representations from
relational data through simple rules or aggregation functions therefore is called
*propositionalization.*

Usually, feature engineering is done manually, by using brute force approaches
or domain knowledge. This process is sometimes also referred to as *data wrangling*.
In any case it is a tedious, time-consuming, and error-prone process. Manual
feature engineering is often done by writing scripts in languages like Python,
R, or SQL.

.. note::

    Unfortunately, the term feature engineering is ambiguous. More often than
    not, feature engineering is meant to describe numerical transformations or
    encoding techniques on a **single** table. The definition used above
    assumes that the raw data comes in relational form, which is true for
    almost all real-world data sets.

Feature learning vs. propositionalization
-----------------------------------------

We follow academia and classify techniques that use simple, merely unconditional
transformations (like aggregations) to construct flat (attribute-value)
representations as `propositionalization approaches
<https://link.springer.com/chapter/10.1007%2F978-3-662-04599-2_11>`_, while we
classify algorithms which directly learn from relational data structures as
*feature learning.* Here, we pick up a term coined in the deep learning context,
where complex relationships are equally learned from raw input data.

getML provides a framework capable of automatically extracting useful and
meaningful features from a relational data model by finding the best merge and
aggregate operations. In fact, the relationships between the target and the
original data is *learned* through one of :ref:`getML's feature learning
algorihtms<feature_learning_algorithms>`.


.. _feature_learning_design:

Design principles
------------------

The general procedure for feature learning on relational data and time
series using getML looks like this:

* The only required input is a :ref:`relational data
  schema<data_model_data_schemes>`. In particular, there needs to be
  some sort of :ref:`target variable(s)<annotating_roles_target>`,
  which shall be predicted. For time series, the schema would
  typically be a
  :ref:`self-join<data_model_time_series_self_join>`. In addition to
  this general information on the data schema, the intended usage of
  the variables has to be provided by setting the
  :ref:`roles<annotating_roles>` of the corresponding columns. How to
  setup a data scheme is described in :ref:`data_model`.

* Features are often of the type (illustrated in pseudo SQL-like
  syntax)
			
  .. code-block:: sql
			   
      COUNT the number of `transactions` within the last X `days`

  where :math:`X` is some sort of fixed numerical value. getML's
  algorithms do identify appropriate values for :math:`X`
  automatically and there is no need for you to provide them by hand.

* Features can also take the form of 
				 
  .. code-block::
					
      COUNT the number of `transactions` for which the `transaction
      type` is ‘type_1’ OR ‘type_2’ OR ’type_3’ OR …

  getML's algorithms also find appropriate conditions based on
  categorical data without any input from the user.

* The feature learning algorithms can handle combinations of
  conditions too. So, features of the form
					  
  .. code-block:: sql
						 
      SOME_AGGREGATION( over some column ) WHERE ( condition_1 AND
	  condition_2 ) OR ( condition_3 AND condition_4 ) OR condition_5
						 
  will be engineered automatically as well. Again, no input from the
  user is required.

* To increase transparency relating to the created features, they can
  be expressed in SQL code. Even though automatically generated
  features will always be less intuitive than hand-crafted ones and
  could be quite complex, we want the user to get an understanding of
  what is going on.

.. _feature_learning_algorithms:

Algorithms
----------

getML contains four powerful feature learning algorithms: 
:class:`~getml.feature_learning.FastProp`, :class:`~getml.feature_learning.Multirel`, 
:class:`~getml.feature_learning.Relboost` and :class:`~getml.feature_learning.RelMT`. 

.. _feature_learning_algorithms_fastprop:

FastProp
++++++++

:class:`~getml.feature_learning.FastProp` is getML's take on propositionalization. 
It is a fast and efficient
implementation utilizing aggregations-based operations, which transform a
relational data structure to a flat table. FastProp allows for the really fast
generation of a substantial number of features based ob simple (unconditional)
aggregations.

A typical FastProp feature looks like this:

.. code-block:: sql

    CREATE TABLE FEATURE_1 AS
    SELECT MAX( t2.column ) AS feature_1,
          t1.rowid AS "rownum"
    FROM "population" t1
    LEFT JOIN "peripheral" t2
    ON t1.join_key = t2.join_key
    WHERE t2.time_stamp <= t1.time_stamp
    ORDER BY t2.time_stamp
    GROUP BY t1.rownum,
             t1.join_key,
             t1.time_stamp;

You may notice that such a feature looks pretty similar to the :ref:`Multirel
feature<feature_learning_algorithms_multirel_feature>` below. And indeed, FastProp
shares some of its :mod:`aggregations <getml.feature_learning.aggregations>`
with Multirel. FastProp features, however, are usually much simpler because they
lack the complex conditions learned by getML's other algorithms (the `WHERE`
statement in the SQL represenation). FastProp is an excellent choice in an
exploration phase of a data science project and delivers decent
results out of the box in many cases. It is recommended that you combine
FastProp with :ref:`mappings`.

.. _feature_learning_algorithms_multirel:

Multirel
++++++++

Simply speaking, :class:`~getml.feature_learning.Multirel` is a more efficient 
variation of
Multi-relational Decision Tree Learning (MRDTL). The core idea is to
minimize redundancies in the original algorithm by incremental
updates. We then combined our improved version of MRDTL with ensemble
learning methods.

MRDTL is a strain of academic literature that was particularly popular in the
early 2000s. It is based on a greedy, tree-like approach:

* Define some sort of objective function that evaluates the quality of your
  feature as it relates to the target variable(s).

* Pick an aggregation and some column to be aggregated.

* Try out different conditions. Keep the one that generates the greatest
  improvement of your objective. Repeat until no improvement can be found or
  some sort of stopping criterion is reached.

The reason this approach has never really taken off outside of
academia is that an efficient implementation is far from
trivial. Most papers on MRDTL implement the algorithm on top of an
existing relational database system, like MySQL.

The main problem with trying to implement something like this on top
of an existing database is that it requires many redundant
operations. Consider a feature like

.. code-block::
			
    COUNT the number of `transactions` in the last X `days`

As we iterate through different values for the threshold :math:`X`,
we are forced to repeat the same operations on the same data over
and over again. Tasks like this bring traditional database engine
to their knees.

The core idea of getML's Multirel algorithm is to minimize
redundancies through `incremental updates`. To allow for incremental
updates and maximal efficiency, we developed a database engine from
scratch in C++. When we evaluate a feature like

.. code-block::

    COUNT the number of `transactions` in the last 90 `days`

and

.. code-block::

    COUNT the number of `transactions` in the last 91 `days`

very little changes in between. Multirel only recalculates what has
changed and keeps everything else untouched. Therefore, it needs two
ingredients that can be incrementally updated: An objective function
and the actual aggregation(s).

Our first ingredient is an objective function that must be suited for 
incremental updates. When we
move from 90 to 91 days, presumably only very few lines in the
:ref:`population table<data_model_population_table>` actually
change. We do not need to recalculate the entire table. In practice,
most commonly used objective functions are fine and this is not much
of a limitation. However, there are some, like rank correlation,
that cannot be used.

The second ingredient, the aggregations, must allow for incremental
updates too. This part is a bit harder, so let us elaborate: Let’s say
we have a match between the population table that contains our targets
and another table (or a self-join). This match happens to be between
the two thresholds 90 and 91 days. As we move from 90 to 91 days, we
have to update our aggregation for that match. For maximum efficiency,
this needs also to be done incrementally. That means we do not want to
recalculate the entire aggregation for all matches that it
aggregates - instead just for the one match in between the two
thresholds.

We want to also support the :code:`AND` and :code:`OR`
combinations of conditions. Therefore, it is possible that a match was *not*
included in the aggregation before, but becomes part of it as we move
the threshold. It is also possible that the match *was* included in
the aggregation, but now it isn’t anymore.

For an aggregation like :const:`~getml.feature_learning.aggregations.Count`,
incremental updates are straight-forward. If the match was not
included, but now it is, then increment by 1. If was included, but it
isn’t anymore, then decrement by 1.

Things are more tricky for aggregations like
:const:`~getml.feature_learning.aggregations.Max`,
:const:`~getml.feature_learning.aggregations.Median`, or
:const:`~getml.feature_learning.aggregations.CountDistinct`. For instance,
whereas incrementing :const:`~getml.feature_learning.aggregations.Max` is easy,
decrementing it is hard. If the match used to be included and is in
fact the maximum value, we now have to find the next biggest
match. And we have to find it quickly - ideally iterating through a
set of thresholds should take linear time in the number of matches. To
make it even more complicated, some cross-joins might result in a lot
of matches, so any data structures that have non-trivial memory
overhead are a no-go.

Everything so far has shed light on how we train *one* feature. But
in practice, we want more than one. So, how do we do that? Since we
are using a tree-based algorithm anyway, we are able to harness the
power of ensemble learning algorithms that have been shown to work
very well with non-relational decision trees, namely bagging and
gradient boosting.

With bagging, we just sample randomly from our population table. We train a
feature on that sample and then pick a different random sample to train the
next feature.

With gradient boosting, we calculate the pseudo-residuals of our
previously trained features. We then train features that predict these
pseudo-residuals.  This procedure guarantees that new features are
targeted and compensate the weaknesses of older ones.

Transpiled to SQL, a typical feature generated by Multirel looks like this:

.. _feature_learning_algorithms_multirel_feature:

.. code-block:: sql
				
    CREATE TABLE FEATURE_1 AS
    SELECT COUNT( * ) AS feature_1,
           t1.join_key,
           t1.time_stamp
    FROM (
         SELECT * ,
                ROW_NUMBER() OVER ( ORDER BY join_key, time_stamp ASC ) AS rownum
         FROM POPULATION
    ) t1
    LEFT JOIN PERIPHERAL t2
    ON t1.join_key = t2.join_key
    WHERE (
       ( t1.time_stamp - t2.time_stamp <= 0.499624 )
    ) AND t2.time_stamp <= t1.time_stamp
    GROUP BY t1.rownum,
             t1.join_key,
             t1.time_stamp;

Further information can be found in the API documentation for
:class:`~getml.feature_learning.Multirel`.

.. _feature_learning_algorithms_relboost:

Relboost
++++++++

:class:`~getml.feature_learning.Relboost` is a generalization of the 
gradient boosting algorithm. More
specifically, it generalizes the xgboost implementation to relational
learning.

The main difference between Relboost and Multirel is that Multirel aggregates
columns, whereas Relboost aggregates *learnable weights*.

Relboost addresses a problem with Multirel that is related to
computational complexity theory: In Multirel, every column can be
aggregated and/or used for generating a condition. That means that the
number of possible features is :math:`\mathcal{O}(n^2)` in the number
of columns in the original tables. As a result having twice as many
columns will lead to a search space that is four times as large (in
reality, it is a bit more complicated than that, but the basic point
is true).

Any computer scientist or applied mathematician will tell you that
:math:`\mathcal{O}(n^2)` is a problem. If you have tables with many
columns, it might turn out to be a problem. Of course, this issue is
not specific to Multirel: It is a very fundamental problem that you
would also have, if you were to write your features by hand or use
brute force.

Relboost offers a way out of this dilemma: Because Relboost aggregates
learnable weights and columns will only be used for conditions, but
not for aggregation. So, now the search space is
:math:`\mathcal{O}(n)` in the number of columns in the original
tables - much better.

This might seem very theoretical, but it has considerable
implications: From our experience with real-world data in various
projects, we know that Relboost usually outperforms Multirel in terms
of predictive accuracy *and* training time.

However, these advantages come at a price: First, the features
generated by Relboost are less intuitive. They are further away from
what you might write by hand, even though they can still be expressed
as SQL code.  Second, it is more difficult to apply Relboost to
:ref:`multiple targets<annotating_roles_target>`, because Relboost has
to learn separate rules and weights for each target.

Expressed as SQL code, a typical feature generated by Relboost looks like this:

.. _feature_learning_algorithms_relboost_feature:

.. code-block:: sql

        CREATE TABLE FEATURE_1 AS
        SELECT SUM(
        CASE
             WHEN ( t1.time_stamp - t2.time_stamp > 0.499624 ) THEN 0.0
             WHEN ( t1.time_stamp - t2.time_stamp <= 0.499624 OR t1.time_stamp IS NULL OR t2.time_stamp IS NULL ) THEN 1.0
             ELSE NULL
        END
        ) AS feature_1,
             t1.join_key,
             t1.time_stamp
        FROM (
             SELECT *,
                    ROW_NUMBER() OVER ( ORDER BY join_key, time_stamp ASC ) AS rownum
             FROM POPULATION
        ) t1
        LEFT JOIN PERIPHERAL t2
        ON t1.join_key = t2.join_key
        WHERE t2.time_stamp <= t1.time_stamp
        GROUP BY t1.rownum,
                 t1.join_key,
                 t1.time_stamp;

Further information can be found in the API documentation for
:class:`~getml.feature_learning.Relboost`.


.. _feature_learning_algorithms_relmt:

RelMT
+++++

:class:`~getml.feature_learning.RelMT` is a generalization of linear 
model trees to relational data. Linear model
trees are decision trees with a linear model at each leaf, resulting in a hybrid
model that combines the strengths of linear models (like interpretability or the
ability to capture linear relationships with those of tree-based algorithms
(like good performance or the ability to capture nonlinear relationships).

RelMT features are particularly well-suited for time-series applications
because time series often carry autoregressive structures, which can be
approximated well by linear models. Think that this month's revenue can
usually be modeled particularly well as a (linear) function of last month's
revenue and so on.  Purely tree-based models often struggle to learn such
relationships because they have fit a piecewise-constant model by predicting the
average of all observations associated with each leaf. Thus it can require a
vast amount of splits to approximate a linear relationship.

Here is a typical RelMT feature:

.. code-block:: sql

        CREATE TABLE FEATURE_1 AS
        SELECT SUM(
        CASE
            WHEN ( t1.time_stamp - t2.time_stamp > 0.499624 ) THEN
        COALESCE( t1.time_stamp - julianday( '1970-01-01' ) - 17202.004, 0.0 ) * -122.121 + COALESCE( t2.column - 3301.156, 0.0 ) * -0.003 
            WHEN ( t1.time_stamp - t2.time_stamp <= 0.499624 OR t1.time_stamp IS NULL OR t2.time_stamp IS NULL ) THEN
        COALESCE( t1.time_stamp - julianday( '1970-01-01' ) - 17202.004, 0.0 ) * 3.654 + COALESCE( t2.column - 3301.156, 0.0 ) * -1.824 + -8.720
             ELSE NULL
        END
        ) AS feature_1,
             t1.join_key,
             t1.time_stamp
        FROM (
             SELECT *,
                    ROW_NUMBER() OVER ( ORDER BY join_key, time_stamp ASC ) AS rownum
             FROM POPULATION
        ) t1
        LEFT JOIN PERIPHERAL t2
        ON t1.join_key = t2.join_key
        WHERE t2.time_stamp <= t1.time_stamp
        GROUP BY t1.rownum,
                 t1.join_key,
                 t1.time_stamp;


RelMT features share some characteristics with Relboost features: Compare the
example feature to the :ref:`Relboost
feature<feature_learning_algorithms_relboost_feature>` above. Both algorithms generate
splits based on a combination of conditions (the `WHEN` part of the `CASE WHEN`
statement above).  But while Relboost learns weights for its leaves (the `THEN`
part of the `CASE WHEN` statement), RelMT learns a linear model, allowing for
linear combinations between columns from the population table and columns of a
certain peripheral table. 
