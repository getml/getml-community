# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
For keeping and versioning data.
"""

from datetime import datetime
from inspect import cleandoc
from typing import Dict, Optional, Union

import pandas as pd

import getml.communication as comm
from getml.data.columns import StringColumn, StringColumnView, from_value
from getml.log import logger
from getml.utilities.formatting import _Formatter

from .data_frame import DataFrame
from .helpers import _is_typed_dict, _make_id
from .helpers2 import (
    _deep_copy,
    _get_last_change,
    _make_subsets_from_kwargs,
    _make_subsets_from_split,
)
from .subset import Subset
from .view import View


class Container:
    """
    A container holds the actual data in the form of a [`DataFrame`][getml.DataFrame] or a [`View`][getml.data.View].

    The purpose of a container is twofold:

    - Assigning concrete data to an abstract [`DataModel`][getml.data.DataModel].

    - Storing data and allowing you to reproduce previous results.

    Attributes:
        population:
            The population table defines the
            [statistical population ](https://en.wikipedia.org/wiki/Statistical_population)
            of the machine learning problem and contains the target variables.

        peripheral:
            The peripheral tables are joined onto *population* or other
            peripheral tables. Note that you can also pass them using
            [`add`][getml.data.Container.add].

        split:
            Contains information on how you want to split *population* into
            different [`Subset`][getml.data.Subset]s.
            Also refer to [`split`][getml.data.split].

        deep_copy:
            Whether you want to create deep copies or your tables.

        train:
            The population table used in the *train*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        validation:
            The population table used in the *validation*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        test:
            The population table used in the *test*
            [`Subset`][getml.data.Subset].
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

        kwargs:
            The population table used in [`Subset`][getml.data.Subset]s
            other than the predefined *train*, *validation* and *test* subsets.
            You can call these subsets anything you want to, and you can access them
            just like *train*, *validation* and *test*.
            You can either pass *population* and *split* or you can pass
            the subsets separately using *train*, *validation*, *test*
            and *kwargs*.

            ??? example
                ```python
                # Pass the subset.
                container = getml.data.Container(my_subset=my_data_frame)

                # You can access the subset just like train,
                # validation or test
                my_pipeline.fit(container.my_subset)
                ```

    ??? example
        A [`DataModel`][getml.data.DataModel] only contains abstract data. When we
        fit a pipeline, we need to assign concrete data.

        This example is taken from the
        [loans notebook ](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/loans.ipynb).
        Note that in the notebook the high level [`StarSchema`][getml.data.StarSchema] implementation is used. For
        demonstration purposes we are proceeding now with the low level implementation.

        ```python
        # The abstract data model is constructed
        # using the DataModel class. A data model
        # does not contain any actual data. It just
        # defines the abstract relational structure.
        dm = getml.data.DataModel(
            population_train.to_placeholder("population")
        )

        dm.add(getml.data.to_placeholder(
            meta=meta,
            order=order,
            trans=trans)
        )

        dm.population.join(
            dm.trans,
            on="account_id",
            time_stamps=("date_loan", "date")
        )

        dm.population.join(
            dm.order,
            on="account_id",
        )

        dm.population.join(
            dm.meta,
            on="account_id",
        )

        # We now have abstract placeholders on something
        # called "population", "meta", "order" and "trans".
        # But how do we assign concrete data? By using
        # a container.
        container = getml.data.Container(
            train=population_train,
            test=population_test
        )

        # meta, order and trans are either
        # DataFrames or Views. Their aliases need
        # to match the names of the placeholders in the
        # data model.
        container.add(
            meta=meta,
            order=order,
            trans=trans
        )

        # Freezing makes the container immutable.
        # This is not required, but often a good idea.
        container.freeze()

        # When we call 'train', the container
        # will return the train set and the
        # peripheral tables.
        my_pipeline.fit(container.train)

        # Same for 'test'
        my_pipeline.score(container.test)
        ```
        If you don't already have a train and test set,
        you can use a function from the
        [`split`][getml.data.split] module.

        ```python

        split = getml.data.split.random(
            train=0.8, test=0.2)

        container = getml.data.Container(
            population=population_all,
            split=split,
        )

        # The remaining code is the same as in
        # the example above. In particular,
        # container.train and container.test
        # work just like above.
        ```

        Containers can also be used for storage and reproducing your
        results.
        A recommended pattern is to assign 'baseline roles' to your data frames
        and then using a [`View`][getml.data.View] to tweak them:

        ```python

        # Assign baseline roles
        data_frame.set_role(["jk"], getml.data.roles.join_key)
        data_frame.set_role(["col1", "col2"], getml.data.roles.categorical)
        data_frame.set_role(["col3", "col4"], getml.data.roles.numerical)
        data_frame.set_role(["col5"], getml.data.roles.target)

        # Make the data frame immutable, so in-place operations are
        # no longer possible.
        data_frame.freeze()

        # Save the data frame.
        data_frame.save()

        # I suspect that col1 leads to overfitting, so I will drop it.
        view = data_frame.drop(["col1"])

        # Insert the view into a container.
        container = getml.data.Container(...)
        container.add(some_alias=view)
        container.save()
        ```

        The advantage of using such a pattern is that it enables you to
        always completely retrace your entire pipeline without creating
        deep copies of the data frames whenever you have made a small
        change like the one in our example. Note that the pipeline will
        record which container you have used.
    """

    def __init__(
        self,
        population: Optional[Union[DataFrame, View]] = None,
        peripheral: Optional[Dict[str, Union[DataFrame, View]]] = None,
        split: Optional[Union[StringColumn, StringColumnView]] = None,
        deep_copy: Optional[bool] = False,
        train: Optional[Union[DataFrame, View]] = None,
        validation: Optional[Union[DataFrame, View]] = None,
        test: Optional[Union[DataFrame, View]] = None,
        **kwargs: Optional[Union[DataFrame, View]],
    ):
        if population is not None and not isinstance(population, (DataFrame, View)):
            raise TypeError(
                "'population' must be a getml.DataFrame or a getml.data.View, got "
                + type(population).__name__
                + "."
            )

        if peripheral is not None and not _is_typed_dict(
            peripheral, str, [DataFrame, View]
        ):
            raise TypeError(
                "'peripheral' must be a dict "
                + "of getml.DataFrames or getml.data.Views."
            )

        if split is not None and not isinstance(
            split, (StringColumn, StringColumnView)
        ):
            raise TypeError(
                "'split' must be StringColumn or a StringColumnView, got "
                + type(split).__name__
                + "."
            )

        if not isinstance(deep_copy, bool):
            raise TypeError(
                "'deep_copy' must be a bool, got " + type(split).__name__ + "."
            )

        exclusive = (population is not None) ^ (
            len(_make_subsets_from_kwargs(train, validation, test, **kwargs)) != 0
        )

        if not exclusive:
            raise ValueError(
                "'population' and 'train', 'validation', 'test' as well as "
                + "other subsets signified by kwargs are mutually exclusive. "
                + "You have to pass "
                + "either 'population' or some subsets, but you cannot pass both."
            )

        if population is None and split is not None:
            raise ValueError(
                "'split's are used for splitting population DataFrames."
                "Hence, if you supply 'split', you also have to supply "
                "a population."
            )

        if population is not None and split is None:
            logger.warning(
                "You have passed a population table without passing 'split'. "
                "You can access the entire set to pass to your pipeline "
                "using the .full attribute."
            )
            split = from_value("full")

        self._id = _make_id()

        self._population = population
        self._peripheral = peripheral or {}
        self._split = split
        self._deep_copy = deep_copy

        # HACK: Do some explicit bookeeping on the subets' length until we have
        # proper endpoint for slice-based subsetting
        if split is not None:
            self._subsets = {}
            self._lengths = {}
            for name, (length, subset) in _make_subsets_from_split(
                population, split
            ).items():
                self._subsets[name] = subset
                self._lengths[name] = length
        else:
            self._subsets = _make_subsets_from_kwargs(train, validation, test, **kwargs)
            self._lengths = {
                name: subset.nrows() for name, subset in self._subsets.items()
            }

        if split is None and not _is_typed_dict(self._subsets, str, [DataFrame, View]):
            raise TypeError(
                "'train', 'validation', 'test' and all other subsets must be either a "
                "getml.DataFrame or a getml.data.View."
            )

        if deep_copy:
            self._population = _deep_copy(self._population, self._id)
            self._peripheral = {
                k: _deep_copy(v, self._id) for (k, v) in self._peripheral.items()
            }
            self._subsets = {
                k: _deep_copy(v, self._id) for (k, v) in self._subsets.items()
            }

        self._last_change = _get_last_change(
            self._population, self._peripheral, self._subsets
        )

        self._frozen_time = None

    def __dir__(self):
        attrs = dir(type(self)) + self._ipython_key_completion()
        attrs = [x for x in attrs if x.isidentifier()]
        return attrs

    def __getattr__(self, key) -> Subset:
        try:
            return self[key]
        except KeyError:
            super().__getattribute__(key)

    def __getitem__(self, key) -> Subset:
        if "_" + key in self.__dict__:
            return self.__dict__["_" + key]

        if key in self.__dict__["_subsets"]:
            if self.__dict__["_deep_copy"] and self._frozen_time is None:
                raise ValueError(
                    cleandoc(
                        f"""
                        If you set deep_copy=True, you must call
                        {type(self).__name__}.freeze() before you can extract data. The
                        idea of deep_copy is to ensure that you can always retrace and
                        reproduce your results. That is why the container
                        needs to be immutable before it can be used.
                        """
                    )
                )

            last_change = _get_last_change(
                self.__dict__["_population"],
                self.__dict__["_peripheral"],
                self.__dict__["_subsets"],
            )

            if self.__dict__["_last_change"] != last_change:
                logger.warning(
                    cleandoc(
                        f"""
                        Some of the data frames added to the {type(self).__name__} have
                        been modified after they were added.  This might lead to
                        unexpected results. To avoid these sort of problems, you can set
                        deep_copy=True when creating the {type(self).__name__}.
                        """
                    )
                )

            return Subset(
                container_id=self.__dict__["_id"],
                population=self.__dict__["_subsets"][key].with_name(key),
                peripheral=self.__dict__["_peripheral"],
            )

        if key in self.__dict__["_peripheral"]:
            return self.__dict__["_peripheral"][key]

        raise KeyError(f"{type(self).__name__} holds no data with name {key!r}.")

    def __repr__(self):
        pop, perph = self._format()

        template = cleandoc(
            """
            population
            {pop}

            peripheral
            {perph}
            """
        )

        return template.format(pop=pop._render_string(), perph=perph._render_string())

    def _repr_html_(self):
        pop, perph = self._format()

        template = cleandoc(
            """
            <div style='margin-top: 15px;'>
            <div style='float: left; margin-right: 50px;'>
            <div style='margin-bottom: 10px; font-size: 1rem;'>population</div>
                {pop}
            </div>
            <div style='float: left;'>
            <div style='margin-bottom: 10px; font-size: 1rem;'>peripheral</div>
                {perph}
            </div>
            </div>
            """
        )

        return template.format(pop=pop._render_html(), perph=perph._render_html())

    def _format(self):
        headers_pop = [["subset", "name", "rows", "type"]]

        rows_pop = [
            [name, subset.name, self._lengths[name], type(subset).__name__]
            for name, subset in self.subsets.items()  # pytype: disable=attribute-error
        ]

        headers_perph = [["name", "rows", "type"]]

        rows_perph = [
            [perph.name, perph.nrows(), type(perph).__name__]
            for perph in self.peripheral.values()  # pytype: disable=attribute-error
        ]

        names = [
            perph.name
            for perph in self.peripheral.values()  # pytype: disable=attribute-error
        ]
        aliases = list(self.peripheral.keys())  # pytype: disable=attribute-error

        if any(alias not in names for alias in aliases):
            headers_perph[0].insert(0, "alias")

            for alias, row in zip(aliases, rows_perph):
                row.insert(0, alias)

        return _Formatter(headers=headers_pop, rows=rows_pop), _Formatter(
            headers=headers_perph, rows=rows_perph
        )

    def _getml_deserialize(self):
        cmd = {k[1:] + "_": v for (k, v) in self.__dict__.items() if v is not None}

        if self._population is not None:
            cmd["population_"] = self._population._getml_deserialize()

        if self._split is not None:
            cmd["split_"] = self._split.cmd

        cmd["peripheral_"] = {
            k: v._getml_deserialize() for (k, v) in self._peripheral.items()
        }

        cmd["subsets_"] = {
            k: v._getml_deserialize() for (k, v) in self._subsets.items()
        }

        return cmd

    def _ipython_key_completion(self):
        attrs = [v[1:] for v in list(vars(self))]
        attrs.extend(self._peripheral)
        if not self._deep_copy or self._frozen_time is not None:
            attrs.extend(self._subsets)
        return attrs

    def __setattr__(self, name, value):
        if not name or name[0] != "_":
            raise ValueError("Attempting a write operation on read-only data.")
        vars(self)[name] = value

    def add(self, *args, **kwargs):
        """
        Adds new peripheral data frames or views.
        """
        wrong_type = [item for item in args if not isinstance(item, (DataFrame, View))]

        if wrong_type:
            raise TypeError(
                "All unnamed arguments must be getml.DataFrames or getml.data.Views."
            )

        wrong_type = [
            k for (k, v) in kwargs.items() if not isinstance(v, (DataFrame, View))
        ]

        if wrong_type:
            raise TypeError(
                "You must pass getml.DataFrames or getml.data.Views, "
                f"but the following arguments were neither: {wrong_type!r}."
            )

        kwargs = {**{item.name: item for item in args}, **kwargs}

        if self._frozen_time is not None:
            raise ValueError(
                f"You cannot add data frames after the {type(self).__name__} has been frozen."
            )

        if self._deep_copy:
            kwargs = {k: _deep_copy(v, self._id) for (k, v) in kwargs.items()}

        self._peripheral = {**self._peripheral, **kwargs}

        self._last_change = _get_last_change(
            self._population, self._peripheral, self._subsets
        )

    def freeze(self):
        """
        Freezes the container, so that changes are no longer possible.

        This is required before you can extract data when `deep_copy=True`. The idea of
        `deep_copy` is to ensure that you can always retrace and reproduce your results.
        That is why the container needs to be immutable before it can be
        used.
        """
        self.sync()
        self._frozen_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def save(self):
        """
        Saves the Container to disk.
        """

        cmd = dict()
        cmd["type_"] = "DataContainer.save"
        cmd["name_"] = self._id

        cmd["container_"] = self._getml_deserialize()

        comm.send(cmd)

    def sync(self):
        """
        Synchronizes the last change with the data to avoid warnings that the data
        has been changed.

        This is only a problem when `deep_copy=False`.
        """
        if self._frozen_time is not None:
            raise ValueError(f"{type(self).__name__} has already been frozen.")
        self._last_change = _get_last_change(
            self._population, self._peripheral, self._subsets
        )

    def to_pandas(self) -> Dict[str, pd.DataFrame]:
        """
        Returns a `Container`'s contents as a dictionary of `pandas.DataFrame`s.
        `name` holds the data frame's `name`, value the data converted to a `pandas.DataFrame`.
        """
        subsets = (
            {name: df.to_pandas() for name, df in self._subsets.items()}
            if self._subsets
            else {}
        )
        peripherals = (
            {name: df.to_pandas() for name, df in self.peripheral.items()}
            if self.peripheral
            else {}
        )
        if subsets or peripherals:
            return {**subsets, **peripherals}

        raise ValueError("Container is empty.")
