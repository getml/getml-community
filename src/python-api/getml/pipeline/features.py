# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Container for the features associated with a pipeline.
"""

from __future__ import annotations

import json
import re
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd  # type: ignore
from numpy.typing import NDArray

import getml.communication as comm
from getml.data.helpers import _is_typed_list
from getml.utilities.formatting import _Formatter

from .dialect import sqlite3, _all_dialects
from .feature import Feature
from .helpers import _attach_empty
from .sql_code import SQLCode
from .sql_string import SQLString

# --------------------------------------------------------------------


class Features:
    """
    Container which holds a pipeline's features. Features can be accessed
    by name, index or with a numpy array. The container supports slicing and
    is sort- and filterable.

    Further, the container holds global methods to request features' importances,
    correlations and their respective transpiled sql representation.

    Note:

        The container is an iterable. So, in addition to
        :meth:`~getml.pipeline.Features.filter` you can also use python list
        comprehensions for filtering.

    Example:
        .. code-block:: python

            all_my_features = my_pipeline.features

            first_feature = my_pipeline.features[0]

            second_feature = my_pipeline.features["feature_1_2"]

            all_but_last_10_features = my_pipeline.features[:-10]

            important_features = [feature for feature in my_pipeline.features if feature.importance > 0.1]

            names, importances = my_pipeline.features.importances()

            names, correlations = my_pipeline.features.correlations()

            sql_code = my_pipeline.features.to_sql()
    """

    # ----------------------------------------------------------------

    def __init__(
        self,
        pipeline: str,
        targets: Sequence[str],
        data: Optional[Sequence[Feature]] = None,
    ) -> None:
        if not isinstance(pipeline, str):
            raise ValueError("'pipeline' must be a str.")

        if not _is_typed_list(targets, str):
            raise TypeError("'targets' must be a list of str.")

        self.pipeline = pipeline

        self.targets = targets

        if data is None:
            self.data = self._load_features()

        else:
            self.data = list(data)

    # ----------------------------------------------------------------

    def __repr__(self) -> str:
        return self._format()._render_string()

    # ------------------------------------------------------------

    def _repr_html_(self) -> str:
        return self._format()._render_html()

    # ----------------------------------------------------------------

    def __getitem__(
        self, key: Union[int, slice, str, NDArray[np.int_]]
    ) -> Union[Feature, Features, Sequence[Feature]]:
        if isinstance(key, int):
            return self.data[key]
        if isinstance(key, slice):
            return self._make_features(self.data[key])
        if isinstance(key, str):
            if key in self.names:
                return [feature for feature in self.data if feature.name == key][0]
            raise AttributeError(f"No Feature with name: {key}")
        if isinstance(key, np.ndarray):
            features_subset = np.array(self.data)[key].tolist()
            return features_subset
        raise TypeError(
            f"Features can only be indexed by: int, slices, or str, not {type(key).__name__}"
        )

    # ----------------------------------------------------------------

    def __iter__(self) -> Iterator[Feature]:
        yield from self.data

    # ----------------------------------------------------------------

    def __len__(self) -> int:
        return len(self.data)

    # ----------------------------------------------------------------

    def _pivot(self, field: str) -> List[Any]:
        """
        Pivots the data for a given field. Returns a list of values of the field's type.
        """
        return [getattr(feature, field) for feature in self.data]

    # ----------------------------------------------------------------

    def _load_features(self) -> List[Feature]:
        """
        Loads the actual feature data from the engine.
        """
        features = []

        for target_num, target in enumerate(self.targets):
            names = self.correlations(target_num, sort=False)[0].tolist()
            indices = range(len(names))
            correlations = _attach_empty(
                self.correlations(target_num, sort=False)[1].tolist(),
                len(names),
                np.NaN,
            )
            importances = _attach_empty(
                self.importances(target_num, sort=False)[1].tolist(), len(names), np.NaN
            )
            sql_transpilations = _attach_empty(
                self.to_sql(subfeatures=False).code[:-1], len(names), ""
            )

            features.extend(
                [
                    Feature(
                        index=index,
                        name=names[index],
                        pipeline=self.pipeline,
                        target=target,
                        targets=self.targets,
                        importance=importances[index],
                        correlation=correlations[index],
                        sql=SQLString(sql_transpilations[index]),
                    )
                    for index in indices
                ]
            )
        return features

    # ----------------------------------------------------------------

    def _format(self) -> _Formatter:
        rows = [
            [
                feature.target,
                feature.name,
                feature.correlation,
                feature.importance,
            ]
            for feature in self.data
        ]

        headers = [["target", "name", "correlation", "importance"]]

        return _Formatter(headers, rows)

    # ----------------------------------------------------------------

    def _make_features(self, data: Sequence[Feature]) -> Features:
        """
        A factory to construct a `Features` container from a list of
        sole `Feature`s.
        """
        return Features(self.pipeline, self.targets, data)

    # ----------------------------------------------------------------

    def _to_pandas(self) -> pd.DataFrame:
        names, correlations, importances, sql, target = (
            self._pivot(field)
            for field in ["name", "correlation", "importance", "sql", "target"]
        )

        data_frame = pd.DataFrame(index=range(len(names)))

        data_frame["names"] = names

        data_frame["correlations"] = correlations

        data_frame["importances"] = importances

        data_frame["target"] = target

        data_frame["sql"] = sql

        return data_frame

    # ----------------------------------------------------------------

    @property
    def correlation(self) -> List[float]:
        """
        Holds the correlations of a :class:`~getml.Pipeline`\ 's features.

        Returns:
            :class:`list` containing the correlations.

        Note:

            The order corresponds to the current sorting of the container.
        """
        return self._pivot("correlation")

    # ------------------------------------------------------------

    def correlations(
        self, target_num: int = 0, sort: bool = True
    ) -> Tuple[NDArray[np.str_], NDArray[np.float_]]:
        """
        Returns the data for the feature correlations,
        as displayed in the getML monitor.

        Args:
            target_num (int):
                Indicates for which target you want to view the
                importances.
                (Pipelines can have more than one target.)

            sort (bool):
                Whether you want the results to be sorted.

        Return:
            (:class:`numpy.ndarray`, :class:`numpy.ndarray`):
                - The first array contains the names of
                  the features.
                - The second array contains the correlations with
                  the target.
        """

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "Pipeline.feature_correlations"
        cmd["name_"] = self.pipeline

        cmd["target_num_"] = target_num

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.engine_exception_handler(msg)
            msg = comm.recv_string(sock)

        json_obj = json.loads(msg)

        names = np.asarray(json_obj["feature_names_"])
        correlations = np.asarray(json_obj["feature_correlations_"])

        assert len(correlations) <= len(names), "Correlations must be <= names"

        if hasattr(self, "data"):
            indices = np.asarray(
                [
                    feature.index
                    for feature in self.data
                    if feature.target == self.targets[target_num]
                    and feature.index < len(correlations)
                ]
            )

            names = names[indices]
            correlations = correlations[indices]

        if not sort:
            return names, correlations

        indices = np.argsort(np.abs(correlations))[::-1]

        return (names[indices], correlations[indices])

    # ----------------------------------------------------------------

    def filter(self, conditional: Callable[[Feature], bool]) -> Features:
        """
        Filters the Features container.

        Args:
            conditional (callable, optional):
                A callable that evaluates to a boolean for a given item.

        Return:
            :class:`getml.pipeline.Features`:
                A container of filtered Features.

        Example:
            .. code-block:: python

                important_features = my_pipeline.features.filter(lambda feature: feature.importance > 0.1)

                correlated_features = my_pipeline.features.filter(lambda feature: feature.correlation > 0.3)

        """
        features_filtered = [feature for feature in self.data if conditional(feature)]
        return Features(self.pipeline, self.targets, data=features_filtered)

    # ----------------------------------------------------------------

    @property
    def importance(self) -> List[float]:
        """
        Holds the correlations of a :class:`~getml.Pipeline`\ 's features.

        Returns:
            :class:`list` containing the correlations.

        Note:

            The order corresponds to the current sorting of the container.
        """
        return self._pivot("importance")

    # ----------------------------------------------------------------

    def importances(
        self, target_num: int = 0, sort: bool = True
    ) -> Tuple[NDArray[np.str_], NDArray[np.float_]]:
        """
        Returns the data for the feature importances,
        as displayed in the getML monitor.

        Args:
            target_num (int):
                Indicates for which target you want to view the
                importances.
                (Pipelines can have more than one target.)

            sort (bool):
                Whether you want the results to be sorted.

        Return:
            (:class:`numpy.ndarray`, :class:`numpy.ndarray`):
                - The first array contains the names of
                  the features.
                - The second array contains their importances.
                  By definition, all importances add up to 1.
        """

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "Pipeline.feature_importances"
        cmd["name_"] = self.pipeline

        cmd["target_num_"] = target_num

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Success!":
                comm.engine_exception_handler(msg)
            msg = comm.recv_string(sock)

        json_obj = json.loads(msg)

        names = np.asarray(json_obj["feature_names_"])
        importances = np.asarray(json_obj["feature_importances_"])

        if hasattr(self, "data"):
            assert len(importances) <= len(names), "Importances must be <= names"

            indices = np.asarray(
                [
                    feature.index
                    for feature in self.data
                    if feature.target == self.targets[target_num]
                    and feature.index < len(importances)
                ]
            )

            names = names[indices]
            importances = importances[indices]

        if not sort:
            return names, importances

        assert len(importances) <= len(names), "Must have the same length"

        indices = np.argsort(importances)[::-1]

        return (names[indices], importances[indices])

    # ----------------------------------------------------------------

    @property
    def name(self) -> List[str]:
        """
        Holds the names of a :class:`~getml.Pipeline`\ 's features.

        Returns:
            :class:`list` containing the names.

        Note:

            The order corresponds to the current sorting of the container.
        """
        return self._pivot("name")

    # ----------------------------------------------------------------

    @property
    def names(self) -> List[str]:
        """
        Holds the names of a :class:`~getml.Pipeline`\ 's features.

        Returns:
            :class:`list` containing the names.

        Note:

            The order corresponds to the current sorting of the container.
        """
        return self._pivot("name")

    # ----------------------------------------------------------------

    def sort(
        self,
        by: Optional[str] = None,
        key: Optional[
            Callable[
                [Feature],
                Union[
                    float,
                    int,
                    str,
                ],
            ]
        ] = None,
        descending: Optional[bool] = None,
    ) -> Features:
        """
        Sorts the Features container. If no arguments are provided the
        container is sorted by target and name.

        Args:
            by (str, optional):
                The name of field to sort by. Possible fields:
                    - name(s)
                    - correlation(s)
                    - importances(s)
            key (callable, optional):
                A callable that evaluates to a sort key for a given item.
            descending (bool, optional):
                Whether to sort in descending order.

        Return:
            :class:`getml.pipeline.Features`:
                A container of sorted Features.

        Example:
            .. code-block:: python

                by_correlation = my_pipeline.features.sort(by="correlation")

                by_importance = my_pipeline.features.sort(key=lambda feature: feature.importance)

        """

        reverse = False if descending is None else descending

        if (by is not None) and (key is not None):
            raise ValueError("Only one of `by` and `key` can be provided.")

        if key is not None:
            features_sorted = sorted(self.data, key=key, reverse=reverse)
            return self._make_features(features_sorted)

        else:
            if by is None:
                features_sorted = sorted(
                    self.data, key=lambda feature: feature.index, reverse=reverse
                )
                features_sorted.sort(key=lambda feature: feature.target)
                return self._make_features(features_sorted)

            if re.match(pattern="names?$", string=by):
                features_sorted = sorted(
                    self.data, key=lambda feature: feature.name, reverse=reverse
                )
                return self._make_features(features_sorted)

            if re.match(pattern="correlations?$", string=by):
                reverse = True if descending is None else descending
                features_sorted = sorted(
                    self.data,
                    key=lambda feature: abs(feature.correlation),
                    reverse=reverse,
                )
                return self._make_features(features_sorted)

            if re.match(pattern="importances?$", string=by):
                reverse = True if descending is None else descending
                features_sorted = sorted(
                    self.data,
                    key=lambda feature: feature.importance,
                    reverse=reverse,
                )
                return self._make_features(features_sorted)

            raise ValueError(f"Cannot sort by: {by}.")

    # ----------------------------------------------------------------

    def to_pandas(self) -> pd.DataFrame:
        """
        Returns all information related to the features in a pandas data frame.
        """

        return self._to_pandas()

    # ----------------------------------------------------------------

    def to_sql(
        self,
        targets: bool = True,
        subfeatures: bool = True,
        dialect: str = sqlite3,
        schema: Optional[str] = None,
        nchar_categorical: int = 128,
        nchar_join_key: int = 128,
        nchar_text: int = 4096,
        size_threshold: Optional[int] = 50000,
    ) -> SQLCode:
        """
        Returns SQL statements visualizing the features.

            Args:
                targets (boolean):
                    Whether you want to include the target columns
                    in the main table.

                subfeatures (boolean):
                    Whether you want to include the code for the
                    subfeatures of a snowflake schema.

                dialect (string):
                    The SQL dialect to use. Must be from
                    :mod:`~getml.pipeline.dialect`. Please
                    note that not all dialects are supported
                    in the getML community edition.

                schema (string, optional):
                    The schema in which to wrap all generated tables and
                    indices. None for no schema. Not applicable to all dialects.
                    For the BigQuery and MySQL dialects, the schema is identical
                    to the database ID.

                nchar_categorical (int):
                    The maximum number of characters used in the
                    VARCHAR for categorical columns. Not applicable
                    to all dialects.

                nchar_join_key (int):
                    The maximum number of characters used in the
                    VARCHAR for join keys. Not applicable
                    to all dialects.

                nchar_text (int):
                    The maximum number of characters used in the
                    VARCHAR for text columns. Not applicable
                    to all dialects.

                size_threshold (int, optional):
                    The maximum number of characters to display
                    in a single feature. Displaying extremely
                    complicated features can crash your iPython
                    notebook or lead to unexpectedly high memory
                    consumption, which is why a reasonable
                    upper limit is advantageous. Set to None
                    for no upper limit.

            Examples:

                .. code-block:: python

                    my_pipeline.features.to_sql()

            Returns:
                :class:`~getml.pipeline.SQLCode`
                    Object representing the features.

            Note:
                Only fitted pipelines
                (:meth:`~getml.Pipeline.fit`) can hold trained
                features which can be returned as SQL statements.

            Note:
                The getML community edition only supports
                transpilation to human-readable SQL. Passing
                'sqlite3' will also produce human-readable SQL.
        """

        if not isinstance(targets, bool):
            raise TypeError("'targets' must be a bool!")

        if not isinstance(subfeatures, bool):
            raise TypeError("'subfeatures' must be a bool!")

        if not isinstance(dialect, str):
            raise TypeError("'dialect' must be a string!")

        if not isinstance(nchar_categorical, int):
            raise TypeError("'nchar_categorical' must be an int!")

        if not isinstance(nchar_join_key, int):
            raise TypeError("'nchar_join_key' must be an int!")

        if not isinstance(nchar_text, int):
            raise TypeError("'nchar_text' must be an int!")

        if dialect not in _all_dialects:
            raise ValueError(
                "'dialect' must from getml.pipeline.dialect, "
                + "meaning that is must be one of the following: "
                + str(_all_dialects)
                + "."
            )

        if size_threshold is not None and not isinstance(size_threshold, int):
            raise TypeError("'size_threshold' must be an int or None!")

        if size_threshold is not None and size_threshold <= 0:
            raise ValueError("'size_threshold' must be a positive number!")

        cmd: Dict[str, Any] = {}

        cmd["type_"] = "Pipeline.to_sql"
        cmd["name_"] = self.pipeline

        cmd["targets_"] = targets
        cmd["subfeatures_"] = subfeatures
        cmd["dialect_"] = dialect
        cmd["schema_"] = schema or ""
        cmd["nchar_categorical_"] = nchar_categorical
        cmd["nchar_join_key_"] = nchar_join_key
        cmd["nchar_text_"] = nchar_text

        if size_threshold is not None:
            cmd["size_threshold_"] = size_threshold

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                comm.engine_exception_handler(msg)
            sql = comm.recv_string(sock)

        return SQLCode(sql.split("\n\n\n"), dialect)
