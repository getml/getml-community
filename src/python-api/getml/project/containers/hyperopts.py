# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Container which holds a project's hyperopts.
"""

from getml.hyperopt.helpers import list_hyperopts
from getml.hyperopt.load_hyperopt import load_hyperopt
from getml.utilities.formatting import _Formatter

# --------------------------------------------------------------------


class Hyperopts:
    """
    Container which holds all hyperopts associated with the currently running
    project. The container supports slicing and is sort- and filterable.
    """

    # ----------------------------------------------------------------

    def __init__(self, data=None):
        self.ids = list_hyperopts()

        if data is None:
            self.data = [load_hyperopt(id) for id in self.ids]
        else:
            self.data = data

    # ----------------------------------------------------------------

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            hyperopts_subset = self.data[key]
            return Hyperopts(data=hyperopts_subset)
        if isinstance(key, str):
            if key in self.ids:
                return [hyperopt for hyperopt in self.data if hyperopt.name == key][0]
            raise AttributeError(f"No Hyperopt with id: {key}")
        raise TypeError(
            f"Hyperopts can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __len__(self):
        return len(self.data)

    # ----------------------------------------------------------------

    def __repr__(self):
        if len(list_hyperopts()) == 0:
            return "No hyperopt in memory."

        return self._format()._render_string()

    # ----------------------------------------------------------------

    def _repr_html_(self):
        if len(list_hyperopts()) == 0:
            return "<p>No hyperopt in memory.</p>"

        return self._format()._render_html()

    # ----------------------------------------------------------------

    def _format(self):
        headers = [["id", "type", "best pipeline"]]

        rows = [
            [self.ids[index], hyperopt.type, hyperopt.best_pipeline.name]
            for index, hyperopt in enumerate(self.data)
        ]

        return _Formatter(headers, rows)

    # ----------------------------------------------------------------

    def filter(self, conditional):
        """
        Filters the hyperopts container.

        Args:
            conditional (callable):
                A callable that evaluates to a boolean for a given item.

        Returns:
            :class:`getml.pipeline.Hyperopts`:
                A container of filtered hyperopts.

        Example:
            .. code-block:: python

                gaussian_hyperopts = getml.project.hyperopts.filter(lamda hyp: "Gaussian" in hyp.type)

        """
        hyperopts_filtered = [
            hyperopt for hyperopt in self.data if conditional(hyperopt)
        ]
        return Hyperopts(data=hyperopts_filtered)

    # ----------------------------------------------------------------

    def sort(self, key, descending=False):
        """
        Sorts the hyperopts container.

        Args:
            key (callable, optional):
                A callable that evaluates to a sort key for a given item.
            descending (bool, optional):
                Whether to sort in descending order.

        Return:
            :class:`getml.pipeline.Hyperopts`:
                A container of sorted hyperopts.

        Example:
            .. code-block:: python

                by_type = getml.project.hyperopt.sort(lambda hyp: hyp.type)

        """
        hyperopts_sorted = sorted(self.data, key=key, reverse=descending)
        return Hyperopts(data=hyperopts_sorted)
