# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Subset class intended to be passed to the pipeline.
"""

from dataclasses import dataclass
from inspect import cleandoc
from typing import Dict, Union

from getml.utilities.formatting import _Formatter

from .data_frame import DataFrame
from .view import View


@dataclass
class Subset:
    """
    A Subset consists of a population table and one or several peripheral tables.

    It is passed by a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
    and [`TimeSeries`][getml.data.TimeSeries] to the [`Pipeline`][getml.Pipeline].

    Attributes:
        container_id:
            The ID of the container the subset belongs to.

        peripheral:
            A dictionary containing the peripheral tables.

        population:
            The population table.

    ??? example
        ```python
        container = getml.data.Container(
            train=population_train,
            test=population_test
        )

        container.add(
            meta=meta,
            order=order,
            trans=trans
        )

        # train and test are Subsets.
        # They contain population_train
        # and population_test respectively,
        # as well as their peripheral tables
        # meta, order and trans.
        my_pipeline.fit(container.train)

        my_pipeline.score(container.test)
        ```
    """

    container_id: str
    peripheral: Dict[str, Union[DataFrame, View]]
    population: Union[DataFrame, View]

    def _format(self):
        headers_perph = [["name", "rows", "type"]]

        rows_perph = [
            [perph.name, perph.nrows(), type(perph).__name__]
            for perph in self.peripheral.values()
        ]

        names = [perph.name for perph in self.peripheral.values()]
        aliases = list(self.peripheral.keys())

        if any(alias not in names for alias in aliases):
            headers_perph[0].insert(0, "alias")

            for alias, row in zip(aliases, rows_perph):
                row.insert(0, alias)

        return self.population._format(), _Formatter(
            headers=headers_perph, rows=rows_perph
        )

    def __repr__(self):
        pop, perph = self._format()
        pop_footer = self.population._collect_footer_data()

        template = cleandoc(
            """
            population
            {pop}

            peripheral
            {perph}
            """
        )

        return template.format(
            pop=pop._render_string(footer=pop_footer), perph=perph._render_string()
        )

    def _repr_html_(self):
        pop, perph = self._format()
        pop_footer = self.population._collect_footer_data()

        template = cleandoc(
            """
            <div>
                <div style='margin-bottom: 10px; font-size: 1rem'>population</div>
                {pop}
            </div>
            <div>
                <div style='margin-bottom: 10px; font-size: 1rem'>peripheral</div>
                {perph}
            </div>
            """
        )

        return template.format(
            pop=pop._render_html(footer=pop_footer), perph=perph._render_html()
        )
