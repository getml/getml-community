# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A gradient boosting model for predicting classification problems.
"""

import numbers
from dataclasses import dataclass
from typing import Any, Dict

import numpy as np

from getml.helpers import _check_parameter_bounds

from .predictor import _Predictor

# --------------------------------------------------------------------


def _validate_xgboost_parameters(parameters: Dict[str, Any]):
    allowed_parameters = {
        "booster",
        "colsample_bylevel",
        "colsample_bytree",
        "early_stopping_rounds",
        "gamma",
        "learning_rate",
        "max_delta_step",
        "max_depth",
        "min_child_weights",
        "n_estimators",
        "n_jobs",
        "external_memory",
        "normalize_type",
        "num_parallel_tree",
        "objective",
        "one_drop",
        "rate_drop",
        "reg_alpha",
        "reg_lambda",
        "sample_type",
        "silent",
        "skip_drop",
        "subsample",
        "type",
    }

    # ----------------------------------------------------------------

    for kkey in parameters:
        if kkey not in allowed_parameters:
            raise KeyError("'unknown XGBoost parameter: " + kkey)

        if kkey == "booster":
            if not isinstance(parameters["booster"], str):
                raise TypeError("'booster' must be of type str")
            if parameters["booster"] not in ["gbtree", "gblinear", "dart"]:
                raise ValueError(
                    "'booster' must either be 'gbtree', 'gblinear', or 'dart'"
                )

        if kkey == "colsample_bylevel":
            if not isinstance(parameters["colsample_bylevel"], numbers.Real):
                raise TypeError("'colsample_bylevel' must be a real number")
            _check_parameter_bounds(
                parameters["colsample_bylevel"],
                "colsample_bylevel",
                [np.finfo(np.float64).resolution, 1.0],  # pylint: disable=E1101
            )

        if kkey == "colsample_bytree":
            if not isinstance(parameters["colsample_bytree"], numbers.Real):
                raise TypeError("'colsample_bytree' must be a real number")
            _check_parameter_bounds(
                parameters["colsample_bytree"],
                "colsample_bytree",
                [np.finfo(np.float64).resolution, 1.0],  # pylint: disable=E1101
            )

        if kkey == "gamma":
            if not isinstance(parameters["gamma"], numbers.Real):
                raise TypeError("'gamma' must be a real number")
            _check_parameter_bounds(
                parameters["gamma"], "gamma", [0.0, np.finfo(np.float64).max]
            )

        if kkey == "learning_rate":
            if not isinstance(parameters["learning_rate"], numbers.Real):
                raise TypeError("'learning_rate' must be a real number")
            _check_parameter_bounds(
                parameters["learning_rate"], "learning_rate", [0.0, 1.0]
            )

        if kkey == "max_delta_step":
            if not isinstance(parameters["max_delta_step"], numbers.Real):
                raise TypeError("'max_delta_step' must be a real number")
            _check_parameter_bounds(
                parameters["max_delta_step"],
                "max_delta_step",
                [0.0, np.finfo(np.float64).max],
            )

        if kkey == "max_depth":
            if not isinstance(parameters["max_depth"], numbers.Real):
                raise TypeError("'max_depth' must be a real number")
            _check_parameter_bounds(
                parameters["max_depth"], "max_depth", [0.0, np.iinfo(np.int32).max]
            )

        if kkey == "min_child_weights":
            if not isinstance(parameters["min_child_weights"], numbers.Real):
                raise TypeError("'min_child_weights' must be a real number")
            _check_parameter_bounds(
                parameters["min_child_weights"],
                "min_child_weights",
                [0.0, np.finfo(np.float64).max],
            )

        if kkey == "n_estimators":
            if not isinstance(parameters["n_estimators"], numbers.Real):
                raise TypeError("'n_estimators' must be a real number")
            _check_parameter_bounds(
                parameters["n_estimators"], "n_estimators", [10, np.iinfo(np.int32).max]
            )

        if kkey == "normalize_type":
            if not isinstance(parameters["normalize_type"], str):
                raise TypeError("'normalize_type' must be of type str")

            if "booster" in parameters and parameters["booster"] == "dart":
                if parameters["normalize_type"] not in ["forest", "tree"]:
                    raise ValueError(
                        "'normalize_type' must either be 'forest' or 'tree'"
                    )

        if kkey == "num_parallel_tree":
            if not isinstance(parameters["num_parallel_tree"], numbers.Real):
                raise TypeError("'num_parallel_tree' must be a real number")
            _check_parameter_bounds(
                parameters["num_parallel_tree"],
                "num_parallel_tree",
                [1, np.iinfo(np.int32).max],
            )

        if kkey == "n_jobs":
            if not isinstance(parameters["n_jobs"], numbers.Real):
                raise TypeError("'n_jobs' must be a real number")
            _check_parameter_bounds(
                parameters["n_jobs"], "n_jobs", [0, np.iinfo(np.int32).max]
            )

        if kkey == "objective":
            if not isinstance(parameters["objective"], str):
                raise TypeError("'objective' must be of type str")
            if parameters["objective"] not in [
                "reg:squarederror",
                "reg:tweedie",
                "reg:linear",
                "reg:logistic",
                "binary:logistic",
                "binary:logitraw",
            ]:
                raise ValueError(
                    """'objective' must either be 'reg:squarederror', """
                    """'reg:tweedie', 'reg:linear', 'reg:logistic', """
                    """'binary:logistic', or 'binary:logitraw'"""
                )

        if kkey == "external_memory":
            if not isinstance(parameters["external_memory"], bool):
                raise TypeError("'external_memory' must be a bool")

        if kkey == "one_drop":
            if not isinstance(parameters["one_drop"], bool):
                raise TypeError("'one_drop' must be a bool")

        if kkey == "rate_drop":
            if not isinstance(parameters["rate_drop"], numbers.Real):
                raise TypeError("'rate_drop' must be a real number")
            _check_parameter_bounds(parameters["rate_drop"], "rate_drop", [0.0, 1.0])

        if kkey == "reg_alpha":
            if not isinstance(parameters["reg_alpha"], numbers.Real):
                raise TypeError("'reg_alpha' must be a real number")
            _check_parameter_bounds(
                parameters["reg_alpha"], "reg_alpha", [0.0, np.finfo(np.float64).max]
            )

        if kkey == "reg_lambda":
            if not isinstance(parameters["reg_lambda"], numbers.Real):
                raise TypeError("'reg_lambda' must be a real number")
            _check_parameter_bounds(
                parameters["reg_lambda"], "reg_lambda", [0.0, np.finfo(np.float64).max]
            )

        if kkey == "sample_type":
            if not isinstance(parameters["sample_type"], str):
                raise TypeError("'sample_type' must be of type str")

            if "booster" in parameters and parameters["booster"] == "dart":
                if parameters["sample_type"] not in ["uniform", "weighted"]:
                    raise ValueError(
                        "'sample_type' must either be 'uniform' or 'weighted'"
                    )

        if kkey == "silent":
            if not isinstance(parameters["silent"], bool):
                raise TypeError("'silent' must be of type bool")

        if kkey == "skip_drop":
            if not isinstance(parameters["skip_drop"], numbers.Real):
                raise TypeError("'skip_drop' must be a real number")
            _check_parameter_bounds(parameters["skip_drop"], "skip_drop", [0.0, 1.0])

        if kkey == "subsample":
            if not isinstance(parameters["subsample"], numbers.Real):
                raise TypeError("'subsample' must be a real number")
            _check_parameter_bounds(
                parameters["subsample"],
                "subsample",
                [np.finfo(np.float64).resolution, 1.0],  # pylint: disable=E1101
            )


# --------------------------------------------------------------------


@dataclass(repr=False)
class XGBoostClassifier(_Predictor):
    """Gradient boosting classifier based on
    `xgboost <https://xgboost.readthedocs.io/en/latest/>`_.

    XGBoost is an implementation of the gradient tree boosting algorithm that
    is widely recognized for its efficiency and predictive accuracy.

    Gradient tree boosting trains an ensemble of decision trees by training
    each tree to predict the *prediction error of all previous trees* in the
    ensemble:

    .. math::

        \\min_{\\nabla f_{t,i}} \\sum_i L(f_{t-1,i} + \\nabla f_{t,i}; y_i),

    where :math:`\\nabla f_{t,i}` is the prediction generated by the
    newest decision tree for sample :math:`i` and :math:`f_{t-1,i}` is
    the prediction generated by all previous trees, :math:`L(...)` is
    the loss function used and :math:`y_i` is the :ref:`target
    <annotating_roles_target>` we are trying to predict.

    XGBoost implements this general approach by adding two specific components:

    1. The loss function :math:`L(...)` is approximated using a Taylor series.

    2. The leaves of the decision tree :math:`\\nabla f_{t,i}` contain weights
       that can be regularized.

    These weights are calculated as follows:

    .. math::

        w_l = -\\frac{\\sum_{i \\in l} g_i}{ \\sum_{i \\in l} h_i + \\lambda},

    where :math:`g_i` and :math:`h_i` are the first and second order derivative
    of :math:`L(...)` w.r.t. :math:`f_{t-1,i}`, :math:`w_l` denotes the weight
    on leaf :math:`l` and :math:`i \\in l` denotes all samples on that leaf.

    :math:`\\lambda` is the regularization parameter `reg_lambda`.
    This hyperparameter can be set by the users or the hyperparameter
    optimization algorithm to avoid overfitting.

    Args:
        booster (string, optional):
            Which base classifier to use.

            Possible values:

            * 'gbtree': normal gradient boosted decision trees
            * 'gblinear': uses a linear model instead of decision trees
            * 'dart': adds dropout to the standard gradient boosting algorithm.
              Please also refer to the remarks on *rate_drop* for further
              explanation on 'dart'.

        colsample_bylevel (float, optional):
            Subsample ratio for the columns used, for each level
            inside a tree.

            Note that XGBoost grows its trees level-by-level, not
            node-by-node.
            At each level, a subselection of the features will be randomly
            picked and the best
            feature for each split will be chosen. This hyperparameter
            determines the share of features randomly picked at each level.
            When set to 1, then now such sampling takes place.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: (0, 1]

        colsample_bytree (float, optional):
            Subsample ratio for the columns used, for each tree.
            This means that for each tree, a subselection
            of the features will be randomly chosen. This hyperparameter
            determines the share of features randomly picked for each tree.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: (0, 1]

        external_memory (bool, optional):
            When the in_memory flag of the engine is set to False,
            XGBoost can use the external memory functionality.
            This reduces the memory consumption,
            but can also affect the quality of the predictions.
            External memory is deactivated by default and it
            is recommended to only use external memory
            for feature selection.
            When the in_memory flag of the engine is set to True,
            (the default value), XGBoost will never use
            external memory.

        gamma (float, optional):
            Minimum loss reduction required for any update
            to the tree. This means that every potential update
            will first be evaluated for its improvement to the loss
            function. If the improvement exceeds gamma,
            the update will be accepted.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`]

        learning_rate (float, optional):
            Learning rate for the gradient boosting algorithm.
            When a new tree :math:`\\nabla f_{t,i}` is trained,
            it will be added to the existing trees
            :math:`f_{t-1,i}`. Before doing so, it will be
            multiplied by the *learning_rate*.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, 1]

        max_delta_step (float, optional):
            The maximum delta step allowed for the weight estimation
            of each tree.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`)

        max_depth (int, optional):
            Maximum allowed depth of the trees.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`]

        min_child_weights (float, optional):
            Minimum sum of weights needed in each child node for a
            split. The idea here is that any leaf should have
            a minimum number of samples in order to avoid overfitting.
            This very common form of regularizing decision trees is
            slightly
            modified to refer to weights instead of number of samples,
            but the basic idea is the same.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`]

        n_estimators (int, optional):
            Number of estimators (trees).

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [10, :math:`\\infty`]


        normalize_type (string, optional):
            This determines how to normalize trees during 'dart'.

            Possible values:

            * 'tree': a new tree has the same weight as a single
              dropped tree.

            * 'forest': a new tree has the same weight as a the sum of
              all dropped trees.

            Please also refer to the remarks on
            *rate_drop* for further explanation.

            Will be ignored if `booster` is not set to 'dart'.

        n_jobs (int, optional):
            Number of parallel threads. When set to zero, then
            the optimal number of threads will be inferred automatically.

            Range: [0, :math:`\\infty`]

        objective (string, optional):
            Specify the learning task and the corresponding
            learning objective.

            Possible values:

            * 'reg:logistic'
            * 'binary:logistic'
            * 'binary:logitraw'

        one_drop (bool, optional):
            If set to True, then at least one tree will always be
            dropped out. Setting this hyperparameter to *true* reduces
            the likelihood of overfitting.

            Please also refer to the remarks on
            *rate_drop* for further explanation.

            Will be ignored if `booster` is not set to 'dart'.

        rate_drop (float, optional):
            Dropout rate for trees - determines the probability
            that a tree will be dropped out. Dropout is an
            algorithm that enjoys considerable popularity in
            the deep learning community. It means that every node can
            be randomly removed during training.

            This approach
            can also be applied to gradient boosting, where it
            means that every tree can be randomly removed with
            a certain probability. Said probability is determined
            by *rate_drop*. Dropout for gradient boosting is
            referred to as the 'dart' algorithm.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Will be ignored if `booster` is not set to 'dart'.

        reg_alpha (float, optional):
            L1 regularization on the weights.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`]

        reg_lambda (float, optional):
            L2 regularization on the weights. Please refer to
            the introductory remarks to understand how this
            hyperparameter influences your weights.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: [0, :math:`\\infty`]

        sample_type (string, optional):
            Possible values:

            * 'uniform': every tree is equally likely to be dropped
              out

            * 'weighted': the dropout probability will be proportional
              to a tree's weight

            Please also refer to the remarks on
            *rate_drop* for further explanation.

            Will be ignored if `booster` is not set to 'dart'.

        silent (bool, optional):
            In silent mode, XGBoost will not print out information on
            the training progress.

        skip_drop (float, optional):
            Probability of skipping the dropout during a given
            iteration. Please also refer to the remarks on
            *rate_drop* for further explanation.

            *Increasing* this hyperparameter reduces the
            likelihood of overfitting.

            Will be ignored if `booster` is not set to 'dart'.

            Range: [0, 1]

        subsample (float, optional):
            Subsample ratio from the training set. This means
            that for every tree a subselection of *samples*
            from the training set will be included into training.
            Please note that this samples *without* replacement -
            the common approach for random forests is to sample
            *with* replace.

            *Decreasing* this hyperparameter reduces the
            likelihood of overfitting.

            Range: (0, 1]
    """

    booster: str = "gbtree"
    colsample_bylevel: float = 1.0
    colsample_bytree: float = 1.0
    early_stopping_rounds: int = 10
    gamma: float = 0.0
    learning_rate: float = 0.1
    max_delta_step: float = 0.0
    max_depth: int = 3
    min_child_weights: float = 1.0
    n_estimators: int = 100
    external_memory: bool = False
    normalize_type: str = "tree"
    num_parallel_tree: int = 1
    n_jobs: int = 1
    objective: str = "binary:logistic"
    one_drop: bool = False
    rate_drop: float = 0.0
    reg_alpha: float = 0.0
    reg_lambda: float = 1.0
    sample_type: str = "uniform"
    silent: bool = True
    skip_drop: float = 0.0
    subsample: float = 1.0

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

                x = getml.predictors.XGBoostClassifier()
                x.gamma = 200
                x.validate()

        Note:

            This method is called at end of the __init__ constructor
            and every time before the predictor - or a class holding
            it as an instance variable - is send to the getML engine.
        """

        # ------------------------------------------------------------

        if params is None:
            params = self.__dict__
        else:
            params = {**self.__dict__, **params}

        if not isinstance(params, dict):
            raise ValueError("params must be None or a dictionary!")

        _validate_xgboost_parameters(params)

        # ------------------------------------------------------------

        if params["objective"] not in [
            "reg:logistic",
            "binary:logistic",
            "binary:logitraw",
        ]:
            raise ValueError(
                """'objective' supported in XGBoostClassifier
                                 are 'reg:logistic', 'binary:logistic',
                                 and 'binary:logitraw'"""
            )
