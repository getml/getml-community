# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A simple logistic regression model for predicting classification problems.
"""

from dataclasses import dataclass

from .linear_regression import _validate_linear_model_parameters
from .predictor import _Predictor

# ------------------------------------------------------------------------------


@dataclass(repr=False)
class LogisticRegression(_Predictor):
    """Simple predictor for classification problems.

    Learns a simple linear relationship using the sigmoid function:

    .. math::

        \\hat{y} = \\sigma(w_0 + w_1 * feature_1 + w_2 * feature_2 + ...)

    :math:`\\sigma` denotes the sigmoid function:

    .. math::

        \\sigma(z) = \\frac{1}{1 + exp(-z)}

    The weights are optimized by minimizing the cross entropy loss of
    the predictions :math:`\\hat{y}` w.r.t. the :ref:`targets
    <annotating_roles_target>` :math:`y`.

    .. math::

        L(\\hat{y},y) = - y*\\log \\hat{y} - (1 - y)*\\log(1 - \\hat{y})

    Logistic regressions are always trained numerically.

    If you decide to pass :ref:`categorical
    features<annotating_roles_categorical>` to the
    :class:`~getml.predictors.LogisticRegression`, it will be trained
    using the Broyden-Fletcher-Goldfarb-Shannon (BFGS) algorithm.
    Otherwise, it will be trained using adaptive moments (Adam). BFGS
    is more accurate, but less scalable than Adam.

    Args:
        learning_rate (float, optional):
            The learning rate used for the Adaptive Moments algorithm
            (only relevant when categorical features are
            included). Range: (0, :math:`\\infty`]

        reg_lambda (float, optional):
            L2 regularization parameter. Range: [0, :math:`\\infty`]

    """

    # ----------------------------------------------------------------

    learning_rate: float = 0.9
    reg_lambda: float = 1e-10

    # ----------------------------------------------------------------

    def validate(self, params=None):
        """Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params (dict, optional): A dictionary containing
                the parameters to validate. If not is passed,
                the own parameters will be validated.

        Examples:

            .. code-block:: python

                l = getml.predictors.LogisticRegression()
                l.learning_rate = 20
                l.validate()

        Note:

            This method is called at end of the __init__ constructor
            and every time before the predictor - or a class holding
            it as an instance variable - is send to the getML engine.
        """

        if params is None:
            params = self.__dict__
        else:
            params = {**self.__dict__, **params}

        if not isinstance(params, dict):
            raise ValueError("params must be None or a dictionary!")

        _validate_linear_model_parameters(params)


# ------------------------------------------------------------------------------
