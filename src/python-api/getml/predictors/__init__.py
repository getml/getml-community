# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""This module contains machine learning algorithms to learn and predict on the
generated features.

The predictor classes defined in this module serve two
purposes. First, a predictor can be used as a ``feature_selector``
in :class:`~getml.Pipeline` to only select the best features
generated during the automated feature learning and to get rid off
any redundancies. Second, by using it as a ``predictor``, it will
be trained on the features of the supplied data set and used to
predict to unknown results. Every time a new data set is passed to
the :meth:`~getml.Pipeline.predict` method of one of the
:mod:`~getml.models`, the raw relational data is interpreted in the
data model, which was provided during the construction of the model,
transformed into features using the trained feature learning
algorithm, and, finally, its :ref:`target<annotating_roles_target>`
will be predicted using the trained predictor.

The algorithms can be grouped according to their finesse and
whether you want to use them for a classification or
regression problem.

.. csv-table::

    "", "simple", "sophisticated"
    "regression", ":class:`~getml.predictors.LinearRegression`", ":class:`~getml.predictors.XGBoostRegressor`"
    "classification", ":class:`~getml.predictors.LogisticRegression`", ":class:`~getml.predictors.XGBoostClassifier`"

Note:

    All predictors need to be passed to :class:`~getml.Pipeline`.
"""


from .scale_gbm_classifier import ScaleGBMClassifier
from .scale_gbm_regressor import ScaleGBMRegressor
from .linear_regression import LinearRegression
from .logistic_regression import LogisticRegression
from .predictor import _Predictor
from .xgboost_classifier import XGBoostClassifier
from .xgboost_regressor import XGBoostRegressor


__all__ = (
    "ScaleGBMClassifier",
    "ScaleGBMRegressor",
    "LinearRegression",
    "LogisticRegression",
    "XGBoostClassifier",
    "XGBoostRegressor",
)


_classification_types = [
    LogisticRegression().type,
    XGBoostClassifier().type,
    ScaleGBMClassifier().type,
]
