# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A simple linear regression model for predicting regression problems.
"""

import numbers
from dataclasses import dataclass

import numpy as np

from getml.helpers import _check_parameter_bounds

from .predictor import _Predictor

# --------------------------------------------------------------------


@dataclass(repr=False)
class LinearRegression(_Predictor):
    """Simple predictor for regression problems.

    Learns a simple linear relationship using ordinary least squares (OLS)
    regression:

    .. math::

        \\hat{y} = w_0 + w_1 * feature_1 + w_2 * feature_2 + ...

    The weights are optimized by minimizing the squared loss of the
    predictions :math:`\\hat{y}` w.r.t. the :ref:`targets
    <annotating_roles_target>` :math:`y`.

    .. math::

        L(y,\\hat{y}) = \\frac{1}{n} \\sum_{i=1}^{n} (y_i -\\hat{y}_i)^2

    Linear regressions can be trained arithmetically or numerically.
    Training arithmetically is more accurate, but suffers worse
    scalability.

    If you decide to pass :ref:`categorical
    features<annotating_roles_categorical>` to the
    :class:`~getml.predictors.LinearRegression`, it will be trained
    numerically. Otherwise, it will be trained arithmetically.

    Args:
        learning_rate (float, optional):
            The learning rate used for training numerically (only
            relevant when categorical features are included). Range:
            (0, :math:`\\infty`]

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

                l = getml.predictors.LinearRegression()
                l.learning_rate = 8.1
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


def _validate_linear_model_parameters(parameters):
    """Checks both the types and values of the `parameters` and raises an
    exception is something is off.

    Examples:

        .. code-block:: python

            getml.helpers.validation._validate_linear_model_parameters(
                {'learning_rate': 0.1})

    Args:
        parameters (dict): Dictionary containing some of all
            parameters supported in
            :class:`~getml.predictors.LinearRegression` and
            :class:`~getml.predictors.LogisticRegression`.

    Note:

        Both :class:`~getml.predictors.LinearRegression` and
        :class:`~getml.predictors.LogisticRegression` have an instance
        variable called ``type``, which is not checked in this
        function but in the corresponding
        :meth:`~getml.predictors.LinearRegression.validate` method. If
        it is supplied to this function, it won't cause harm but will
        be ignored instead of checked.
    """

    allowed_parameters = {"learning_rate", "reg_lambda", "type"}

    # ----------------------------------------------------------------

    for kkey in parameters:

        if kkey not in allowed_parameters:
            raise KeyError("'unknown parameter: " + kkey)

        if kkey == "learning_rate":
            if not isinstance(parameters["learning_rate"], numbers.Real):
                raise TypeError("'learning_rate' must be a real number")
            _check_parameter_bounds(
                parameters["learning_rate"],
                "learning_rate",
                [
                    np.finfo(np.float64).resolution,  # pylint: disable=E1101
                    np.finfo(np.float64).max,
                ],
            )

        if kkey == "reg_lambda":
            if not isinstance(parameters["reg_lambda"], numbers.Real):
                raise TypeError("'reg_lambda' must be a real number")
            _check_parameter_bounds(
                parameters["reg_lambda"], "reg_lambda", [0.0, np.finfo(np.float64).max]
            )


# --------------------------------------------------------------------
