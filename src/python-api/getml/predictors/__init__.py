# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""

The predictor classes defined in this module **serve two purposes**.

First, a predictor can be used as a **feature selector**
in [`Pipeline`][getml.Pipeline] to only select the best features
generated during the automated feature learning and to get rid of
any redundancies.

Second, by using it as a **predictor**, it will
be trained on the features of the supplied data set and used to
predict unknown results.

Every time a new data set is passed to
the [`predict`][getml.Pipeline.predict] method of one of the
models, the raw relational data is interpreted in the
data model, which was provided during the construction of the model,
transformed into features using the trained feature learning
algorithm, and, finally, its [target][annotating-data-target]
will be predicted using the trained predictor.

The algorithms can be grouped according to their finesse and
whether you want to use them for a classification or
regression problem. For memory intensive applications, the getML
Enterprise edition offers predictors with memory mapping .




|   | simple  | sophisticated  | memory intensive  |
----|---------|---------------|--------------------|
|  __regression__ | [`LinearRegression`][getml.predictors.LinearRegression]  |  [`XGBoostRegressor`][getml.predictors.XGBoostRegressor] | [`ScaleGBMRegressor`][getml.predictors.ScaleGBMRegressor] |
| __classification__ |  [`LogisticRegression`][getml.predictors.LogisticRegression] | [`XGBoostClassifier`][getml.predictors.XGBoostClassifier]  | [`ScaleGBMClassifier`][getml.predictors.ScaleGBMClassifier] |


Note:
    All predictors need to be passed to [`Pipeline`][getml.Pipeline].
"""

from .linear_regression import LinearRegression
from .logistic_regression import LogisticRegression
from .predictor import _Predictor
from .scale_gbm_classifier import ScaleGBMClassifier
from .scale_gbm_regressor import ScaleGBMRegressor
from .xgboost_classifier import XGBoostClassifier
from .xgboost_regressor import XGBoostRegressor

__all__ = (
    "ScaleGBMClassifier",
    "ScaleGBMRegressor",
    "LinearRegression",
    "LogisticRegression",
    "_Predictor",
    "XGBoostClassifier",
    "XGBoostRegressor",
)


_classification_types = [
    LogisticRegression().type,
    XGBoostClassifier().type,
    ScaleGBMClassifier().type,
]
