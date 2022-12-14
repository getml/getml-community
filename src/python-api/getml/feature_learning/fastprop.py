# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Feature learning based on propositionalization.
"""

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Literal, Optional, Union

from .aggregations import _Aggregations
from .aggregations import fastprop as fastprop_aggregations
from .feature_learner import _FeatureLearner
from .validation import _validate_dfs_model_parameters

# --------------------------------------------------------------------


@dataclass(repr=False)
class FastProp(_FeatureLearner):
    """
    Generates simple features based on propositionalization.

    :class:`~getml.feature_learning.FastProp` generates simple and easily
    interpretable features for relational data and time series. It is based on a
    propositionalization approach and has been optimized for speed and memory efficiency.
    :class:`~getml.feature_learning.FastProp` generates a large number
    of features and selects the most relevant ones based on the pair-wise correlation
    with the target(s).

    It is recommended to combine :class:`~getml.feature_learning.FastProp` with
    the :class:`~getml.preprocessors.Mapping` and :class:`~getml.preprocessors.Seasonal`
    preprocessors, which can drastically improve predictive accuracy.

    Args:
        aggregation (List[:class:`~getml.feature_learning.aggregations`], optional):
            Mathematical operations used by the automated feature
            learning algorithm to create new features.

            Must be from :mod:`~getml.feature_learning.aggregations`.

        delta_t (float, optional):
            Frequency with which lag variables will be explored in a
            time series setting. When set to 0.0, there will be no lag
            variables. Please note that you must also pass a value to
            max_lag.

            For more information please refer to
            :ref:`data_model_time_series`. Range: [0, :math:`\\infty`]

        loss_function (:class:`~getml.feature_learning.loss_functions`, optional):
            Objective function used by the feature learning algorithm
            to optimize your features. For regression problems use
            :class:`~getml.feature_learning.loss_functions.SquareLoss` and for
            classification problems use
            :class:`~getml.feature_learning.loss_functions.CrossEntropyLoss`.

        max_lag (int, optional):
            Maximum number of steps taken into the past to form lag variables. The
            step size is determined by delta_t. Please note that you must also pass
            a value to delta_t.

            For more information please refer to
            :ref:`data_model_time_series`. Range: [0, :math:`\\infty`]

        min_df (int, optional):
            Only relevant for columns with role :const:`~getml.data.roles.text`.
            The minimum
            number of fields (i.e. rows) in :const:`~getml.data.roles.text` column a
            given word is required to appear in to be included in the bag of words.
            Range: [1, :math:`\\infty`]

        num_features (int, optional):
            Number of features generated by the feature learning
            algorithm. Range: [1, :math:`\\infty`]

        n_most_frequent (int, optional):
            :class:`~getml.feature_learning.FastProp` can find the N most frequent
            categories in a categorical column and derive features from them.
            The parameter determines how many categories should be used.
            Range: [0, :math:`\\infty`]

        num_threads (int, optional):
            Number of threads used by the feature learning algorithm. If set to
            zero or a negative value, the number of threads will be
            determined automatically by the getML engine. Range:
            [:math:`0`, :math:`\\infty`]

        sampling_factor (float, optional):
            FastProp uses a bootstrapping procedure (sampling with replacement) to train
            each of the features. The sampling factor is proportional to the share of
            the samples randomly drawn from the population table every time Multirel
            generates a new feature. A lower sampling factor (but still greater than
            0.0), will lead to less danger of overfitting, less complex statements and
            faster training. When set to 1.0, roughly 2,000 samples are drawn from the
            population table. If the population table contains less than 2,000 samples,
            it will use standard bagging. When set to 0.0, there will be no sampling at
            all. Range: [0, :math:`\\infty`]

        silent (bool, optional):
            Controls the logging during training.

        vocab_size (int, optional):
            Determines the maximum number
            of words that are extracted in total from :const:`getml.data.roles.text`
            columns. This can be interpreted as the maximum size of the bag of words.
            Range: [0, :math:`\\infty`]

    """

    # ----------------------------------------------------------------

    agg_sets: ClassVar[_Aggregations] = fastprop_aggregations

    # ----------------------------------------------------------------

    aggregation: List[str] = field(
        default_factory=lambda: fastprop_aggregations.Default
    )
    delta_t: float = 0.0
    loss_function: Optional[Literal["CrossEntropyLoss", "SquareLoss"]] = None
    max_lag: int = 0
    min_df: int = 30
    n_most_frequent: int = 0
    num_features: int = 200
    num_threads: int = 0
    sampling_factor: float = 1.0
    silent: bool = True
    vocab_size: int = 500

    # ----------------------------------------------------------------

    def validate(self, params: Optional[Dict[str, Any]] = None) -> None:
        """
        Checks both the types and the values of all instance
        variables and raises an exception if something is off.

        Args:
            params (dict, optional):
                A dictionary containing the parameters to validate.
                params can hold the full set or a subset of the
                parameters explained in
                :class:`~getml.feature_learning.FastProp`.
                If params is None, the
                current set of parameters in the
                instance dictionary will be validated.

        """
        # ------------------------------------------------------------

        if params is None:
            params = self.__dict__
        else:
            params = {**self.__dict__, **params}

        if not isinstance(params, dict):
            raise ValueError("params must be None or a dictionary!")

        # ------------------------------------------------------------

        for kkey in params:
            if kkey not in type(self)._supported_params:
                raise KeyError(
                    f"Instance variable '{kkey}' is not supported in {self.type}."
                )

        # ------------------------------------------------------------

        _validate_dfs_model_parameters(**params)


# --------------------------------------------------------------------
