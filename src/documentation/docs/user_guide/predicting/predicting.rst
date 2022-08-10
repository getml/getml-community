.. _predicting:

Predicting
==========

Now that you know how to :ref:`engineer a flat table of
features<feature_engineering>`, you are ready to make predictions of
the :ref:`target variable(s)<annotating_roles_target>`. 

Using getML
-----------

getML comes with four built-in machine learning :mod:`~getml.predictors`:

* :class:`~getml.predictors.LinearRegression`
* :class:`~getml.predictors.LogisticRegression`
* :class:`~getml.predictors.XGBoostClassifier`
* :class:`~getml.predictors.XGBoostRegressor`  

Using one of them in your analysis is very simple. Just pass one as
:code:`predictor` argument to either :class:`~getml.pipeline.Pipeline` on initialization.

.. code-block:: python
    
    feature_learner1 = getml.feature_learners.Relboost()
    
    feature_learner2 = getml.feature_learners.Multirel()

    predictor = getml.predictors.XGBoostRegressor()

    pipe = getml.pipeline.Pipeline(
        data_model=data_model,
        peripheral=peripheral_placeholder,
        feature_learners=[feature_learner1, feature_learner2],
        predictors=predictor,
    )

When you  call :meth:`~getml.pipeline.Pipeline.fit`, the entire
pipeline will be trained.

Note that :class:`~getml.pipeline.Pipeline` comes with dependency
tracking. That means it can figure out on its own what has changed
and what needs to be trained again.

.. code-block:: python
    
    feature_learner1 = getml.feature_learners.Relboost()
    
    feature_learner2 = getml.feature_learners.Multirel()

    predictor = getml.predictors.XGBoostRegressor()

    pipe = getml.pipeline.Pipeline(
        data_model=data_model,
        population=population_placeholder,
        peripheral=peripheral_placeholder,
        feature_learners=[feature_learner1, feature_learner2],
        predictors=predictor 
    )

    pipe.fit(...)

    pipe.predictors[0].n_estimators = 50

    # Only the predictor has changed,
    # so only the predictor will be refitted.
    pipe.fit(...)

To score the performance of your prediction on a test
data set, the getML models come with a
:meth:`~getml.pipeline.Pipeline.score` method. The available
metrics are documented in :mod:`~getml.pipeline.scores`.

To use a trained model, including both the trained features and the
predictor, to make predictions on new, unseen data, call the
:meth:`~getml.pipeline.Pipeline.predict` method of your model.

Using external software
-----------------------

In our experience the most relevant contribution to making accurate
predictions are the generated features. Before trying to tweak your
analysis by using sophisticated prediction algorithms and tuning their
hyperparameters, we recommend tuning the hyperparameters of
your :class:`~getml.feature_learning.Multirel` or
:class:`~getml.feature_learning.Relboost` instead. You can do so either by
hand (see :ref:`feature_engineering_best_hyperparameters`) or using
getML's automated :ref:`hyperparameter optimization<hyperopt>`.

If you wish to use external predictors, you can transform new data,
which is compliant with your relational data model, to a flat feature
table using the :meth:`~getml.pipeline.Pipeline.transform` method
of your pipeline.

