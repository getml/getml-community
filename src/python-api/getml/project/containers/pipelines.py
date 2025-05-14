# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Container which holds all of a project's pipelines.
"""

from __future__ import annotations

from typing import Any, Callable, List

from getml.pipeline.helpers2 import _refresh_all, list_pipelines
from getml.pipeline.metrics import accuracy, auc, cross_entropy, mae, rmse, rsquared
from getml.utilities.formatting import _Formatter

# --------------------------------------------------------------------


class Pipelines:
    """
    Container which holds all pipelines associated with the currently running
    project. The container supports slicing and is sort- and filterable.

    ??? example
        Show the first 10 pipelines belonging to the current project:
        ```python
        getml.project.pipelines[:10]
        ```
        You can use nested list comprehensions to retrieve a scoring history
        for your project:
        ```python
        import matplotlib.pyplot as plt

        hyperopt_scores = [(score.date_time, score.mae) for pipe in getml.project.pipelines
                              for score in pipe.scores["data_test"]
                              if "hyperopt" in pipe.tags]

        fig, ax = plt.subplots()
        ax.bar(*zip(*hyperopt_scores))
        ```
    """

    # ----------------------------------------------------------------

    def __init__(self, data=None):
        self.ids = list_pipelines()

        if data is None:
            self.data = _refresh_all()
        else:
            self.data = data

    # ----------------------------------------------------------------

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            pipelines_subset = self.data[key]
            return Pipelines(data=pipelines_subset)
        if isinstance(key, str):
            if key in self.ids:
                return [pipeline for pipeline in self.data if pipeline.id == key][0]
            raise AttributeError(f"No Pipeline with id: {key}")
        raise TypeError(
            f"Pipelines can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ----------------------------------------------------------------

    def __repr__(self):
        if len(self.ids) == 0:
            return "No pipelines in memory."

        return self._format()._render_string()

    # ----------------------------------------------------------------

    def _repr_html_(self):
        if len(self.ids) == 0:
            return "<p>No pipelines in memory.</p>"
        return self._format()._render_html()

    # ----------------------------------------------------------------

    @property
    def _contains_regresion_pipelines(self):
        return any(pipe.is_regression for pipe in self.data)

    # ----------------------------------------------------------------

    @property
    def _contains_classification_pipelines(self):
        return any(pipe.is_classification for pipe in self.data)

    # ----------------------------------------------------------------

    def _format(self):
        scores: List[Any] = []
        scores_headers: List[Any] = []

        if self._contains_classification_pipelines:
            scores.extend(
                [
                    pipeline._scores.get(accuracy, []),
                    pipeline._scores.get(auc, []),
                    pipeline._scores.get(cross_entropy, []),
                ]
                for pipeline in self.data
            )

            scores_headers.extend([accuracy, auc, cross_entropy])

        if self._contains_regresion_pipelines:
            scores.extend(
                [
                    pipeline._scores.get(mae, []),
                    pipeline._scores.get(rmse, []),
                    pipeline._scores.get(rsquared, []),
                ]
                for pipeline in self.data
            )

            scores_headers.extend([mae, rmse, rsquared])

        if (
            self._contains_classification_pipelines
            and self._contains_regresion_pipelines
        ):
            scores = [
                [*classf, *reg]
                for classf, reg in zip(
                    scores[: len(self.data)], scores[len(self.data) :]
                )
            ]

        sets_used = [pipeline._scores.get("set_used", "") for pipeline in self.data]

        targets = [pipeline.targets for pipeline in self.data]

        feature_learners = [
            [feature_learner.type for feature_learner in pipeline.feature_learners]
            for pipeline in self.data
        ]

        tags = [pipeline.tags for pipeline in self.data]

        headers = [
            [
                "id",
                "tags",
                "feature learners",
                "targets",
                *scores_headers,
                "set used",
            ]
        ]

        rows = [
            [
                pipeline.id,
                tags[index],
                feature_learners[index],
                targets[index],
                *scores[index],
                sets_used[index],
            ]
            for index, pipeline in enumerate(self.data)
        ]

        # ------------------------------------------------------------

        return _Formatter(headers, rows)

    # ----------------------------------------------------------------

    def sort(self, key: Callable, descending: bool = False) -> Pipelines:
        """
        Sorts the pipelines container.

        Args:
            key:
                A callable that evaluates to a sort key for a given item.

            descending:
                Whether to sort in descending order.

        Returns:
                A container of sorted pipelines.

        ??? example
            ```python
            by_auc = getml.project.pipelines.sort(key=lambda pipe: pipe.auc)
            by_fl = getml.project.pipelines.sort(key=lambda pipe: pipe.feature_learners[0].type)
            ```
        """
        pipelines_sorted = sorted(self.data, key=key, reverse=descending)
        return Pipelines(data=pipelines_sorted)

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable) -> Pipelines:
        """
        Filters the pipelines container.

        Args:
            conditional:
                A callable that evaluates to a boolean for a given item.

        Returns:
                A container of filtered pipelines.

        ??? example
            ```python
            pipelines_with_tags = getml.project.pipelines.filter(lambda pipe: len(pipe.tags) > 0)
            accurate_pipes = getml.project.pipelines.filter(lambda pipe: all(acc > 0.9 for acc in pipe.accuracy))
            ```
        """
        pipelines_filtered = [
            pipeline for pipeline in self.data if conditional(pipeline)
        ]

        return Pipelines(data=pipelines_filtered)
