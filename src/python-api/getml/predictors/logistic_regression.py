# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A simple logistic regression model for predicting classification problems.
"""

from dataclasses import dataclass
from typing import Optional

from .linear_regression import _validate_linear_model_parameters
from .predictor import _Predictor

# ------------------------------------------------------------------------------


@dataclass(repr=False)
class LogisticRegression(_Predictor):
    """Simple predictor for classification problems.

    Learns a simple linear relationship using the sigmoid function:

    $$
    \\hat{y} = \\sigma(w_0 + w_1 * feature_1 + w_2 * feature_2 + ...)
    $$

    $\\sigma$ denotes the sigmoid function:

    $$
    \\sigma(z) = \\frac{1}{1 + exp(-z)}
    $$

    The weights are optimized by minimizing the cross entropy loss of
    the predictions $\\hat{y}$ w.r.t. the [targets][annotating-data-target] $y$.

    $$
    L(\\hat{y},y) = - y*\\log \\hat{y} - (1 - y)*\\log(1 - \\hat{y})
    $$

    Logistic regressions are always trained numerically.

    If you decide to pass categorical
    features: `annotating_roles_categorical` to the
    [`LogisticRegression`][getml.predictors.LogisticRegression], it will be trained
    using the Broyden-Fletcher-Goldfarb-Shannon (BFGS) algorithm.
    Otherwise, it will be trained using adaptive moments (Adam). BFGS
    is more accurate, but less scalable than Adam.

    Args:
        learning_rate:
            The learning rate used for the Adaptive Moments algorithm
            (only relevant when categorical features are
            included). Range: (0, ∞]

        reg_lambda:
            L2 regularization parameter. Range: [0, ∞]
    """

    # ----------------------------------------------------------------

    learning_rate: float = 0.9
    reg_lambda: float = 1e-10

    # ----------------------------------------------------------------

    def validate(self, params: Optional[dict] = None):
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params: A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.

        Examples:
            ```python
            l = getml.predictors.LogisticRegression()
            l.learning_rate = 20
            l.validate()
            ```

        Note:
            This method is called at end of the `__init__` constructor
            and every time before the predictor - or a class holding
            it as an instance variable - is sent to the getML Engine.
        """

        if params is None:
            params = self.__dict__
        else:
            params = {**self.__dict__, **params}

        if not isinstance(params, dict):
            raise ValueError("params must be None or a dictionary!")

        _validate_linear_model_parameters(params)


# ------------------------------------------------------------------------------
