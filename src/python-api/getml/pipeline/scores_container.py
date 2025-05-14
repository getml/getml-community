# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
A container for storing a pipeline's scoring history.
"""

from __future__ import annotations

from typing import Callable, Dict, List, Sequence, Union

from getml.utilities.formatting import _Formatter

from .helpers import _unlist_maybe
from .metrics import _all_metrics, accuracy, auc, cross_entropy, mae, rmse, rsquared
from .score import ClassificationScore, Score


class Scores:
    """
    Container which holds the history of all scores associated with a given pipeline.
    The container supports slicing and is sort- and filterable.

    Args:
        data:
            A list of [`Score`][getml.pipeline.score] objects.
        latest:
            A dictionary containing the latest scores for each metric.
    """

    # ----------------------------------------------------------------

    def __init__(self, data: Sequence[Score], latest: Dict[str, List[float]]) -> None:
        self._latest = latest

        self.is_classification = all(
            isinstance(score, ClassificationScore) for score in data
        )

        self.is_regression = not self.is_classification

        self.data = data

        self.sets_used = [score.set_used for score in data]

    # ----------------------------------------------------------------

    def __getitem__(self, key: Union[int, slice, str]):
        if isinstance(key, int):
            return self.data[key]

        if isinstance(key, slice):
            scores_subset = self.data[key]
            return Scores(scores_subset, self._latest)

        if isinstance(key, str):
            # allow to access latest scores via their name for backward compatibility
            if key in _all_metrics:
                return self._latest[key]

            scores_subset = [score for score in self.data if score.set_used == key]

            return Scores(scores_subset, self._latest)

        raise TypeError(
            f"Scores can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __iter__(self):
        yield from self.data

    # ----------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.data)

    # ------------------------------------------------------------

    def __repr__(self) -> str:
        return self._format()._render_string()

    # ------------------------------------------------------------

    def _repr_html_(self) -> str:
        return self._format()._render_html()

    # ------------------------------------------------------------

    def _format(self) -> _Formatter:
        headers = ["date time", "set used", "target"]
        if self.is_classification:
            headers += ["accuracy", "auc", "cross entropy"]
        if self.is_regression:
            headers += ["mae", "rmse", "rsquared"]

        rows = [list(vars(score).values()) for score in self.data]

        return _Formatter([headers], rows)

    # ----------------------------------------------------------------

    @property
    def accuracy(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `accuracy` from the latest scoring run.
        """
        return _unlist_maybe(self._latest[accuracy])

    # ----------------------------------------------------------------

    @property
    def auc(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `auc` from the latest scoring run.
        """
        return _unlist_maybe(self._latest[auc])

    # ----------------------------------------------------------------

    @property
    def cross_entropy(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `cross entropy` from the latest scoring run.
        """
        return _unlist_maybe(self._latest[cross_entropy])

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable[[Score], bool]) -> Scores:
        """
        Filters the scores container.

        Args:
            conditional:
                A callable that evaluates to a boolean for a given item.

        Returns:
                A container of filtered scores.

        ??? example
            ```python
            from datetime import datetime, timedelta
            one_week_ago = datetime.today() - timedelta(days=7)
            scores_last_week = pipe.scores.filter(lambda score: score.date_time >= one_week_ago)
            ```
        """
        scores_filtered = [score for score in self.data if conditional(score)]

        return Scores(scores_filtered, self._latest)

    # ----------------------------------------------------------------

    @property
    def mae(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `mae` from the latest scoring run.

        Returns:
                The mean absolute error.
        """
        return _unlist_maybe(self._latest[mae])

    # ----------------------------------------------------------------

    @property
    def rmse(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `rmse` from the latest scoring run.

        Returns:
                The root mean squared error.
        """
        return _unlist_maybe(self._latest[rmse])

    # ----------------------------------------------------------------

    @property
    def rsquared(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the `rsquared` from the latest scoring run.

        Returns:
                The squared correlation coefficient.
        """
        return _unlist_maybe(self._latest[rsquared])

    # ----------------------------------------------------------------

    def sort(
        self, key: Callable[[Score], Union[float, int, str]], descending: bool = False
    ) -> Scores:
        """
        Sorts the scores container.

        Args:
            key:
                A callable that evaluates to a sort key for a given item.

            descending:
                Whether to sort in descending order.

        Returns:
                A container of sorted scores.

        ??? example
            ```python
            by_auc = pipe.scores.sort(key=lambda score: score.auc)
            most_recent_first = pipe.scores.sort(key=lambda score: score.date_time, descending=True)
            ```
        """

        scores_sorted = sorted(self.data, key=key, reverse=descending)
        return Scores(scores_sorted, self._latest)
