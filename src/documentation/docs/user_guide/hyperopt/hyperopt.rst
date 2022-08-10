.. _hyperopt:

Hyperparameter optimization
===========================

In the sections on :ref:`feature_engineering` and :ref:`predicting` we learned how
to train both the feature learning algorithm and the machine learning algorithm
used for prediction in the getML engine. However, there are lots of parameters
involved. :class:`~getml.feature_learning.Multirel`,
:class:`~getml.feature_learning.Relboost`,
:class:`~getml.feature_learning.RelMT`,
:class:`~getml.feature_learning.FastProp`,
:class:`~getml.predictors.LinearRegression`,
:class:`~getml.predictors.LogisticRegression`,
:class:`~getml.predictors.XGBoostClassifier`, and
:class:`~getml.predictors.XGBoostRegressor` all have their own
settings. That is why you might want to use  hyperparameter
optimization.

The most relevant parameters of these classes can be chosen to
constitute individual dimensions of a parameter space. For each
parameter, a lower and upper bound has to be provided and the
hyperparameter optimization will search the space within these
bounds. This will be done iteratively by drawing a specific parameter
combination, overwriting the corresponding parameters in a base pipeline,
and then fitting and scoring it. The algorithm used to draw from the
parameter space is represented by the different classes of
:mod:`~getml.hyperopt`. 

While :class:`~getml.hyperopt.RandomSearch` and
:class:`~getml.hyperopt.LatinHypercubeSearch` are purely statistical
approaches, :class:`~getml.hyperopt.GaussianHyperparameterSearch` 
uses prior knowledge obtained from evaluations of previous parameter
combinations to select the next one.

.. _hyperopt_tuning:

Tuning routines 
---------------

The easiest way to conduct a hyperparameter optimization in getML are the
tuning routines :func:`~getml.hyperopt.tune_feature_learners` 
and :func:`~getml.hyperopt.tune_predictors`. They roughly work as follows:

- They begin with a base pipeline, in which the default parameters for the
  feature learner or the predictor are used.

- They then proceed by optimizing 2 or 3 parameters at a time using a 
  :class:`~getml.hyperopt.GaussianHyperparameterSearch`. If the best pipeline
  outperforms the base pipeline, the best pipeline becomes the new base pipeline.

- Taking the base pipeline from the previous steps, the tuning routine then optimizes
  the next 2 or 3 hyperparameters. If the best pipeline from that hyperparameter
  optimization outperforms the current base pipeline, that pipeline becomes the
  new base pipeline.

- These steps are then repeated for more hyperparameters.

The following table lists the tuning recipes and hyperparameter subspaces for each step.

.. _hyperopt_tuning_subspaces:

Tuning recipes and hyperparameter subspaces
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. csv-table:: Tuning recipes for predictors
   :header: "Predictor","Stage","Hyperparameter","Subspace"

   :class:`~getml.predictors.LinearRegression`; :class:`~getml.predictors.LogisticRegression`   ,1 (base parameters),reg_lambda,"[1E-11, 100]"
   ,,learning_rate,"[0.5, 0.99]"
   ,,,
   :class:`~getml.predictors.XGBoostClassifier`; :class:`~getml.predictors.XGBoostRegressor`,1 (base parameters),learning_rate,"[0.05, 0.3]"
   ,2 (tree parameters),max_depth,"[1, 15]"
   ,,min_child_weights,"[1, 6]"
   ,,gamma,"[0, 5]"
   ,3 (sampling parameters),colsample_bytree,"[0.75, 0.9]"
   ,,subsample,"[075, 0.9]"
   ,4 (regularization parameters),reg_alpha,"[0, 5]"
   ,,reg_lambda,"[0, 10]"

.. csv-table:: Tuning recipes for feature learners
   :header: "Feature Learner","Stage","Hyperparameter","Subspace"

   :class:`~getml.feature_learning.FastProp`,1 (base parameters),num_features,"[50, 500]"
   ,,n_most_frequent,"[0, 20]"
   ,,,
   :class:`~getml.feature_learning.Multirel`,1 (base parameters),num_features,"[10, 50]"
   ,,shrinkage,"[0, 0.3]"
   ,2 (tree parameters),max_length,"[0, 10]"
   ,,min_num_samples,"[1, 500]"
   ,3 (regularization parameters),share_aggregations,"[0.1, 0.5]"
   ,,,
   :class:`~getml.feature_learning.Relboost`,1 (base parameters),num_features,"[10, 50]"
   ,,shrinkage,"[0, 0.3]"
   ,2 (tree parameters),max_length,"[0, 10]"
   ,,min_num_samples,"[1, 500]"
   ,3 (regularization parameters),share_aggregations,"[0.1, 0.5]"
   ,,,
   :class:`~getml.feature_learning.RelMT`,1 (base parameters),num_features,"[10, 50]"
   ,,shrinkage,"[0, 0.3]"
   ,2 (tree parameters),max_depth,"[1, 8]"
   ,,min_num_samples,"[1, 500]"
   ,3 (regularization parameters),reg_lambda,"[0, 0.0001]"

The advantage of the tuning routines is that they provide a convenient out-of-the-box 
experience for hyperparameter tuning. For most use cases, it is sufficient to tune the XGBoost 
predictor.

More advanced users can rely on the more low-level hyperparameter optimization routines.

.. _hyperopt_random:

Random search 
-------------

A :class:`~getml.hyperopt.RandomSearch` draws random hyperparameter
sets from the hyperparameter space.

.. _hyperopt_latin:

Latin hypercube search 
----------------------

A :class:`~getml.hyperopt.LatinHypercubeSearch` draws almost random 
hyperparameter sets from the hyperparameter space, but ensures
that they are sufficiently different from each other.

.. _hyperopt_bayesian:

Gaussian hyperparameter search
------------------------------

A :class:`~getml.hyperopt.GaussianHyperparameterSearch` search works like this:

- It begins with a burn-in phase, usually about 70% to 90% of all iterations. 
  During that burn-in phase, the hyperparameter space is sampled more or less at random,
  using either a random search or a latin hypercube search. 
  You can control this phase using ``ratio_iter`` and ``surrogate_burn_in_algorithm``.
  
- Once enough information has been collected, it fits a Gaussian process on the 
  hyperparameters with the score we want to maximize or minimize as the predicted 
  variable. Note that the Gaussian process has hyperparameters itself, which are 
  also optimized. You can control this phase using ``gaussian_kernel``, 
  ``gaussian_optimization_algorithm``, ``gaussian_optimization_burn_in_algorithm`` and 
  ``gaussian_optimization_burn_ins``.

- It then uses the Gaussian process to predict the expected information (EI). The EI is a
  measure of how much additional information it might get from evaluating a particular 
  point in the hyperparameter space. The expected information is to be maximized. 
  The point in the hyperparameter space with the maximum expected information is the next 
  point that is actually evaluated (meaning a new pipeline with these hyperparameters 
  is trained). You can control this phase using ``optimization_algorithm``, 
  ``optimization_burn_ins`` and ``optimization_burn_in_algorithm``.

In a nutshell, the GaussianHyperparameterSearch behaves like human data scientists:

- At first, it picks random hyperparameter combinations.

- Once it has gained a better understanding of the hyperparameter space, 
  it starts evaluating hyperparameter combinations that are particularly 
  interesting.

