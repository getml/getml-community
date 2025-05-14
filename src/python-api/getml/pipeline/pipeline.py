# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
This submodule contains the Pipeline, which is the main
class for feature learning and prediction.
"""

from __future__ import annotations

import copy
import json
import numbers
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Union

import numpy as np
from numpy.typing import NDArray
from rich import print

import getml.communication as comm
from getml import data
from getml.data import (
    DataModel,
    Placeholder,
    View,
    _decode_data_model,
    _decode_placeholder,
)
from getml.data.data_frame import DataFrame
from getml.data.helpers import (
    _is_subclass_list,
    _is_typed_list,
    _remove_trailing_underscores,
)
from getml.feature_learning import (
    Fastboost,
    FastProp,
    Multirel,
    Relboost,
    RelMT,
    _FeatureLearner,
)
from getml.feature_learning.loss_functions import _classification_loss
from getml.predictors import (
    LinearRegression,
    LogisticRegression,
    ScaleGBMClassifier,
    ScaleGBMRegressor,
    XGBoostClassifier,
    XGBoostRegressor,
    _classification_types,
    _Predictor,
)
from getml.preprocessors import (
    CategoryTrimmer,
    EmailDomain,
    Imputation,
    Mapping,
    Seasonal,
    Substring,
    TextFieldSplitter,
)
from getml.preprocessors.preprocessor import _Preprocessor
from getml.utilities.formatting import _SignatureFormatter

from .columns import Columns
from .features import Features
from .helpers import (
    _check_df_types,
    _handle_loss_function,
    _infer_peripheral,
    _make_id,
    _parse_fe,
    _parse_metadata,
    _parse_pred,
    _parse_preprocessor,
    _print_time_taken,
    _replace_with_nan_maybe,
    _transform_peripheral,
)
from .issues import Issues
from .metadata import AllMetadata
from .metrics import (
    _all_metrics,
    _classification_metrics,
    accuracy,
    auc,
    cross_entropy,
    mae,
    rmse,
    rsquared,
)
from .plots import Plots
from .score import ClassificationScore, RegressionScore
from .scores_container import Scores
from .tables import Tables
from .tags import Tags

NOT_FITTED = "NOT FITTED"


class Pipeline:
    """
    A Pipeline is the main class for feature learning and prediction.

    Args:
        data_model:
            Abstract representation of the data_model,
            which defines the abstract relationships between the tables.
            Required for the feature learners.

        peripheral:
            Abstract representations of the additional tables used to
            augment the information provided in `population`. These
            have to be the same objects that were
            [`join`][getml.data.Placeholder.join] ed onto the
            `population` [`Placeholder`][getml.data.Placeholder].
            Their order determines the order of the
            peripheral [`DataFrame`][getml.DataFrame] passed to
            the 'peripheral_tables' argument in
            [`check`][getml.Pipeline.check],
            [`fit`][getml.Pipeline.fit],
            [`predict`][getml.Pipeline.predict],
            [`score`][getml.Pipeline.score], and
            [`transform`][getml.Pipeline.transform], if you
            pass the data frames as a list.
            If you omit the peripheral placeholders, they will
            be inferred from the data model and ordered
            alphabetically.

        preprocessors:
            The preprocessor(s) to be used.
            Must be from [`preprocessors`][getml.preprocessors].
            A single preprocessor does not have to be wrapped in a list.

        feature_learners:
            The feature learner(s) to be used.
            Must be from [`feature_learning`][getml.feature_learning].
            A single feature learner does not have to be wrapped
            in a list.

        feature_selectors:
            Predictor(s) used to select the best features.
            Must be from [`predictors`][getml.predictors].
            A single feature selector does not have to be wrapped
            in a list.
            Make sure to also set *share_selected_features*.

        predictors:
            Predictor(s) used to generate the predictions.
            If more than one predictor is passed, the predictions
            generated will be averaged.
            Must be from [`predictors`][getml.predictors].
            A single predictor does not have to be wrapped
            in a list.

        loss_function:
            The loss function to use for the feature learners.

        tags: Tags exist to help you organize your pipelines.
            You can add any tags that help you remember what you were
            trying to do.

        include_categorical:
            Whether you want to pass categorical columns
            in the population table to the predictor.

        share_selected_features:
            The share of features you want the feature
            selection to keep. When set to 0.0, then all features will be kept.

    ??? example
        We assume that you have already set up your
        preprocessors (refer to [`preprocessors`][getml.preprocessors]),
        your feature learners (refer to [`feature_learning`][getml.feature_learning])
        as well as your feature selectors and predictors
        (refer to [`predictors`][getml.predictors], which can be used
        for prediction and feature selection).

        You might also want to refer to
        [`DataFrame`][getml.DataFrame], [`View`][getml.data.View],
        [`DataModel`][getml.data.DataModel], [`Container`][getml.data.Container],
        [`Placeholder`][getml.data.Placeholder] and
        [`StarSchema`][getml.data.StarSchema].

        If you want to create features for a time series problem,
        the easiest way to do so is to use the [`TimeSeries`][getml.data.TimeSeries]
        abstraction.

        Note that this example is taken from the
        [robot notebook ](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/robot.ipynb).

        ```python
        # All rows before row 10500 will be used for training.
        split = getml.data.split.time(data_all, "rowid", test=10500)

        time_series = getml.data.TimeSeries(
            population=data_all,
            time_stamps="rowid",
            split=split,
            lagged_targets=False,
            memory=30,
        )

        pipe = getml.Pipeline(
            data_model=time_series.data_model,
            feature_learners=[...],
            predictors=...
        )

        pipe.check(time_series.train)

        pipe.fit(time_series.train)

        pipe.score(time_series.test)

        # To generate predictions on new data,
        # it is sufficient to use a Container.
        # You don't have to recreate the entire
        # TimeSeries, because the abstract data model
        # is stored in the pipeline.
        container = getml.data.Container(
            population=population_new,
        )

        # Add the data as a peripheral table, for the
        # self-join.
        container.add(population=population_new)

        predictions = pipe.predict(container.full)
        ```

    ??? example
        If your data can be organized in a simple star schema,
        you can use [`StarSchema`][getml.data.StarSchema].
        [`StarSchema`][getml.data.StarSchema] unifies
        [`Container`][getml.data.Container] and [`DataModel`][getml.data.DataModel]:

        Note that this example is taken from the
        [loans notebook ](https://nbviewer.getml.com/github/getml/getml-demo/blob/master/loans.ipynb).

        ```python

        # First, we insert our data into a StarSchema.
        # population_train and population_test are either
        # DataFrames or Views. The population table
        # defines the statistical population of your
        # machine learning problem and contains the
        # target variables.
        star_schema = getml.data.StarSchema(
            train=population_train,
            test=population_test
        )

        # meta, order and trans are either
        # DataFrames or Views.
        # Because this is a star schema,
        # all joins take place on the population
        # table.
        star_schema.join(
            trans,
            on="account_id",
            time_stamps=("date_loan", "date")
        )

        star_schema.join(
            order,
            on="account_id",
        )

        star_schema.join(
            meta,
            on="account_id",
        )

        # Now you can insert your data model,
        # your preprocessors, feature learners,
        # feature selectors and predictors
        # into the pipeline.
        # Note that the pipeline only knows
        # the abstract data model, but hasn't
        # seen the actual data yet.
        pipe = getml.Pipeline(
            data_model=star_schema.data_model,
            preprocessors=[mapping],
            feature_learners=[fast_prop],
            feature_selectors=[feature_selector],
            predictors=predictor,
        )

        # Now, we pass the actual data.
        # This passes 'population_train' and the
        # peripheral tables (meta, order and trans)
        # to the pipeline.
        pipe.check(star_schema.train)

        pipe.fit(star_schema.train)

        pipe.score(star_schema.test)
        ```

    ??? example
        [`StarSchema`][getml.data.StarSchema] is simpler,
        but cannot be used for more complex data models.
        The general approach is to use
        [`Container`][getml.data.Container] and [`DataModel`][getml.data.DataModel]:

        ```python

        # First, we insert our data into a Container.
        # population_train and population_test are either
        # DataFrames or Views.
        container = getml.data.Container(
            train=population_train,
            test=population_test
        )

        # meta, order and trans are either
        # DataFrames or Views. They are given
        # aliases, so we can refer to them in the
        # DataModel.
        container.add(
            meta=meta,
            order=order,
            trans=trans
        )

        # Freezing makes the container immutable.
        # This is not required, but often a good idea.
        container.freeze()

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

        # Now you can insert your data model,
        # your preprocessors, feature learners,
        # feature selectors and predictors
        # into the pipeline.
        # Note that the pipeline only knows
        # the abstract data model, but hasn't
        # seen the actual data yet.
        pipe = getml.Pipeline(
            data_model=dm,
            preprocessors=[mapping],
            feature_learners=[fast_prop],
            feature_selectors=[feature_selector],
            predictors=predictor,
        )

        # This passes 'population_train' and the
        # peripheral tables (meta, order and trans)
        # to the pipeline.
        pipe.check(container.train)

        pipe.fit(container.train)

        pipe.score(container.test)
        ```

        Technically, you don't actually have to use a
        [`Container`][getml.data.Container]. You might as well do this
        (in fact, a [`Container`][getml.data.Container] is just
        syntactic sugar for this approach):

        ```python

        pipe.check(
            population_train,
            {"meta": meta, "order": order, "trans": trans},
        )

        pipe.fit(
            population_train,
            {"meta": meta, "order": order, "trans": trans},
        )

        pipe.score(
            population_test,
            {"meta": meta, "order": order, "trans": trans},
        )
        ```
        Or you could even do this. The order of the peripheral tables
        can be inferred from the `__repr__` method of the pipeline,
        and it is usually in alphabetical order.

        ```python
        pipe.check(
            population_train,
            [meta, order, trans],
        )

        pipe.fit(
            population_train,
            [meta, order, trans],
        )

        pipe.score(
            population_test,
            [meta, order, trans],
        )
        ```
    """

    def __init__(
        self,
        data_model: Optional[DataModel] = None,
        peripheral: Optional[List[Placeholder]] = None,
        preprocessors: Optional[
            Union[
                CategoryTrimmer,
                EmailDomain,
                Imputation,
                Mapping,
                Seasonal,
                Substring,
                TextFieldSplitter,
                List[
                    Union[
                        CategoryTrimmer,
                        EmailDomain,
                        Imputation,
                        Mapping,
                        Seasonal,
                        Substring,
                        TextFieldSplitter,
                    ]
                ],
            ],
        ] = None,
        feature_learners: Optional[
            Union[
                Union[Fastboost, FastProp, Multirel, Relboost, RelMT],
                List[Union[Fastboost, FastProp, Multirel, Relboost, RelMT]],
            ]
        ] = None,
        feature_selectors: Optional[
            Union[
                Union[
                    LinearRegression,
                    LogisticRegression,
                    XGBoostClassifier,
                    XGBoostRegressor,
                    ScaleGBMClassifier,
                    ScaleGBMRegressor,
                ],
                List[
                    Union[
                        LinearRegression,
                        LogisticRegression,
                        XGBoostClassifier,
                        XGBoostRegressor,
                        ScaleGBMClassifier,
                        ScaleGBMRegressor,
                    ]
                ],
            ],
        ] = None,
        predictors: Optional[
            Union[
                LinearRegression,
                LogisticRegression,
                XGBoostClassifier,
                XGBoostRegressor,
                ScaleGBMClassifier,
                ScaleGBMRegressor,
                List[
                    Union[
                        LinearRegression,
                        LogisticRegression,
                        XGBoostClassifier,
                        XGBoostRegressor,
                        ScaleGBMClassifier,
                        ScaleGBMRegressor,
                    ]
                ],
            ]
        ] = None,
        loss_function: Optional[str] = None,
        tags: Optional[List[str]] = None,
        include_categorical: bool = False,
        share_selected_features: float = 0.5,
    ) -> None:
        data_model = data_model or DataModel("population")

        if not isinstance(data_model, DataModel):
            raise TypeError("'data_model' must be a getml.data.DataModel.")

        peripheral = peripheral or _infer_peripheral(data_model.population)

        preprocessors = preprocessors or []

        feature_learners = feature_learners or []

        feature_selectors = feature_selectors or []

        predictors = predictors or []

        tags = tags or []

        if not isinstance(preprocessors, list):
            preprocessors = [preprocessors]

        if not isinstance(feature_learners, list):
            feature_learners = [feature_learners]

        if not isinstance(feature_selectors, list):
            feature_selectors = [feature_selectors]

        if not isinstance(predictors, list):
            predictors = [predictors]

        if not isinstance(peripheral, list):
            peripheral = [peripheral]

        if not isinstance(tags, list):
            tags = [tags]

        self._id: str = NOT_FITTED

        self.type = "Pipeline"

        loss_function = (
            loss_function
            or (
                [fl.loss_function for fl in feature_learners if fl.loss_function]
                or ["SquareLoss"]
            )[0]
        )

        feature_learners = [
            _handle_loss_function(fl, loss_function) for fl in feature_learners
        ]

        self.data_model = data_model
        self.feature_learners = feature_learners
        self.feature_selectors = feature_selectors
        self.include_categorical = include_categorical
        self.loss_function = loss_function
        self.peripheral = peripheral
        self.predictors = predictors
        self.preprocessors = preprocessors
        self.share_selected_features = share_selected_features
        self.tags = Tags(tags)

        self._metadata: Optional[AllMetadata] = None

        self._scores: Dict[str, Any] = {}

        self._targets: List[str] = []

        setattr(type(self), "_supported_params", list(self.__dict__.keys()))

        self._validate()

    # ----------------------------------------------------------------

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Pipeline):
            raise TypeError("A Pipeline can only be compared to another Pipeline")

        if len(set(self.__dict__.keys())) != len(set(other.__dict__.keys())):
            return False

        for kkey in self.__dict__:
            if kkey not in other.__dict__:
                return False

            # Take special care when comparing numbers.
            if isinstance(self.__dict__[kkey], numbers.Real):
                if not np.isclose(self.__dict__[kkey], other.__dict__[kkey]):
                    return False

            elif self.__dict__[kkey] != other.__dict__[kkey]:
                return False

        return True

    # ----------------------------------------------------------------

    def __repr__(self) -> str:
        obj_dict = self._make_object_dict()

        sig = _SignatureFormatter(data=obj_dict)
        repr_str = sig._format()

        if self.fitted:
            url = self._make_url()
            repr_str += "\n\nurl: " + url if url else ""

        return repr_str

    # ----------------------------------------------------------------

    def _repr_html_(self) -> str:
        obj_dict = self._make_object_dict()

        sig = _SignatureFormatter(data=obj_dict)
        repr_str = sig._format()
        html = f"<pre>{repr_str}</pre>"

        if self.fitted:
            url = self._make_url()
            html += (
                (
                    "<br><pre>"
                    + "url: <a href='"
                    + url
                    + '\' target="_blank">'
                    + url
                    + "</a>"
                    + "</pre>"
                )
                if url
                else ""
            )

        return html

    # ------------------------------------------------------------

    def _check_classification_or_regression(self) -> bool:
        """
        Checks whether there are inconsistencies in the algorithms used
        (mixing classification and regression algorithms).
        """

        all_classifiers = all(
            [
                elem.loss_function in _classification_loss
                for elem in self.feature_learners
            ]
        )

        all_classifiers = all_classifiers and all(
            [elem.type in _classification_types for elem in self.feature_selectors]
        )

        all_classifiers = all_classifiers and all(
            [elem.type in _classification_types for elem in self.predictors]
        )

        all_regressors = all(
            [
                elem.loss_function not in _classification_loss
                for elem in self.feature_learners
            ]
        )

        all_regressors = all_regressors and all(
            [elem.type not in _classification_types for elem in self.feature_selectors]
        )

        all_regressors = all_regressors and all(
            [elem.type not in _classification_types for elem in self.predictors]
        )

        if not all_classifiers and not all_regressors:
            raise ValueError(
                """You are mixing classification and regression
                                algorithms. Please make sure that your feature learning
                                algorithms consistently have classification loss functions
                                (like CrossEntropyLoss) or consistently have regression
                                loss functions (like SquareLoss). Also make sure that your
                                feature selectors and predictors are consistently classifiers
                                (like XGBoostClassifier or LogisticRegression) or consistently
                                regressors (like XGBoostRegressor or LinearRegression).
                             """
            )

        return all_classifiers

    # ------------------------------------------------------------

    def _check_whether_fitted(self) -> None:
        if not self.fitted:
            raise ValueError("Pipeline has not been fitted!")

    # ------------------------------------------------------------

    def _close(self, sock: socket.socket) -> None:
        if not isinstance(sock, socket.socket):
            raise TypeError("'sock' must be a socket.")

        cmd = dict()
        cmd["type_"] = self.type + ".close"
        cmd["name_"] = self.id

        comm.send_string(sock, json.dumps(cmd))

        msg = comm.recv_string(sock)

        if msg != "Success!":
            comm.handle_engine_exception(msg)

    # ------------------------------------------------------------

    def _get_latest_score(self, score: str) -> List[float]:
        nan_ = [np.nan] * len(self.targets)

        if score not in _all_metrics:
            raise AttributeError(f"Not a valid score name: {score}")

        if not self.scored:
            return nan_

        if self.is_classification and score not in _classification_metrics:
            return nan_

        if self.is_regression and score in _classification_metrics:
            return nan_

        return self._scores[score]

    # ------------------------------------------------------------

    def _getml_deserialize(self) -> Dict[str, Any]:
        """
        Expresses the pipeline in a form the Engine can understand.
        """

        cmd = dict()

        self_dict = self.__dict__

        cmd["name_"] = self.id

        for key, value in self_dict.items():
            cmd[key + "_"] = value

        del cmd["_id_"]
        del cmd["_metadata_"]
        del cmd["_scores_"]
        del cmd["_targets_"]

        return cmd

    # ----------------------------------------------------------------

    def _make_object_dict(self) -> Dict[str, Any]:
        obj_dict = copy.deepcopy(self.__dict__)

        obj_dict["data_model"] = self.data_model.population.name

        obj_dict["peripheral"] = [elem.name for elem in self.peripheral]

        obj_dict["preprocessors"] = [elem.type for elem in self.preprocessors]

        obj_dict["feature_learners"] = [elem.type for elem in self.feature_learners]

        obj_dict["feature_selectors"] = [elem.type for elem in self.feature_selectors]

        obj_dict["predictors"] = [elem.type for elem in self.predictors]

        return obj_dict

    # ----------------------------------------------------------------

    def _make_score_history(self) -> List[Union[ClassificationScore, RegressionScore]]:
        scores: List[Dict[str, Any]] = self._scores["history"]
        scores = [_replace_with_nan_maybe(score) for score in scores]

        if self.is_classification:
            return [
                ClassificationScore(
                    date_time=datetime.strptime(
                        score.get("date_time", ""), "%Y-%m-%d %H:%M:%S"
                    ),
                    set_used=score.get("set_used", ""),
                    target=target,
                    accuracy=score.get(accuracy, [np.nan])[target_num],
                    auc=score.get(auc, [np.nan])[target_num],
                    cross_entropy=score.get(cross_entropy, [np.nan])[target_num],
                )
                for score in scores
                for target_num, target in enumerate(self.targets)
            ]

        return [
            RegressionScore(
                date_time=datetime.strptime(
                    score.get("date_time", ""), "%Y-%m-%d %H:%M:%S"
                ),
                set_used=score.get("set_used", ""),
                target=target,
                mae=score.get(mae, [np.nan])[target_num],
                rmse=score.get(rmse, [np.nan])[target_num],
                rsquared=score.get(rsquared, [np.nan])[target_num],
            )
            for score in scores
            for target_num, target in enumerate(self.targets)
        ]

    # ----------------------------------------------------------------

    def _make_url(self) -> Optional[str]:
        url = comm._monitor_url()
        if not url:
            return None
        url += "getpipeline/" + comm._get_project_name() + "/" + self.id + "/0/"
        return url

    # ----------------------------------------------------------------

    def _parse_cmd(self, json_obj: Dict[str, Any]) -> Pipeline:
        ptype = json_obj["type_"]

        del json_obj["type_"]

        if ptype != "Pipeline":
            raise ValueError("Expected type 'Pipeline', got '" + ptype + "'.")

        preprocessors = [
            _parse_preprocessor(elem) for elem in json_obj["preprocessors_"]
        ]

        del json_obj["preprocessors_"]

        feature_learners = [_parse_fe(elem) for elem in json_obj["feature_learners_"]]

        del json_obj["feature_learners_"]

        feature_selectors = [
            _parse_pred(elem) for elem in json_obj["feature_selectors_"]
        ]

        del json_obj["feature_selectors_"]

        predictors = [_parse_pred(elem) for elem in json_obj["predictors_"]]

        del json_obj["predictors_"]

        data_model = _decode_data_model(json_obj["data_model_"])

        del json_obj["data_model_"]

        peripheral = [_decode_placeholder(elem) for elem in json_obj["peripheral_"]]

        del json_obj["peripheral_"]

        id_ = json_obj["name_"]

        del json_obj["name_"]

        kwargs = _remove_trailing_underscores(json_obj)

        self.__init__(
            data_model=data_model,
            peripheral=peripheral,
            preprocessors=preprocessors,
            feature_learners=feature_learners,
            feature_selectors=feature_selectors,
            predictors=predictors,
            **kwargs,
        )

        self._id = id_

        return self

    # ----------------------------------------------------------------

    def _parse_json_obj(self, all_json_objs: Dict[str, Any]) -> Pipeline:
        obj = all_json_objs["obj"]

        scores = all_json_objs["scores"]

        targets = all_json_objs["targets"]

        self._parse_cmd(obj)

        scores = _remove_trailing_underscores(scores)
        scores = _replace_with_nan_maybe(scores)

        self._scores = scores

        self._targets = targets

        peripheral_metadata = [
            _parse_metadata(m) for m in all_json_objs["peripheral_metadata"]
        ]
        population_metadata = _parse_metadata(all_json_objs["population_metadata"])

        self._metadata = AllMetadata(
            peripheral=peripheral_metadata,
            population=population_metadata,
        )

        return self

    # ----------------------------------------------------------------

    def _save(self) -> None:
        """
        Saves the pipeline as a JSON file.
        """

        cmd = dict()
        cmd["type_"] = self.type + ".save"
        cmd["name_"] = self.id

        comm.send(cmd)

    # ------------------------------------------------------------

    def _send(self, additional_tags: Optional[List[str]] = None) -> Pipeline:
        self._validate()

        self._id = _make_id()

        cmd = self._getml_deserialize()

        if additional_tags is not None:
            cmd["tags_"] += additional_tags

        comm.send(cmd)

        return self

    # ------------------------------------------------------------

    def _transform(
        self,
        peripheral_data_frames: Sequence[Union[DataFrame, View]],
        population_data_frame: Union[DataFrame, View],
        sock: socket.socket,
        score: bool = False,
        predict: bool = False,
        df_name: str = "",
        table_name: str = "",
    ) -> Union[NDArray[np.float_], None]:
        _check_df_types(population_data_frame, peripheral_data_frames)

        if not isinstance(sock, socket.socket):
            raise TypeError("'sock' must be a socket.")

        if not isinstance(score, bool):
            raise TypeError("'score' must be of type bool")

        if not isinstance(predict, bool):
            raise TypeError("'predict' must be of type bool")

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be of type str")

        if not isinstance(df_name, str):
            raise TypeError("'df_name' must be of type str")

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".transform"
        cmd["name_"] = self.id

        cmd["score_"] = score
        cmd["predict_"] = predict

        cmd["peripheral_dfs_"] = [
            df._getml_deserialize() for df in peripheral_data_frames
        ]
        cmd["population_df_"] = population_data_frame._getml_deserialize()

        cmd["df_name_"] = df_name
        cmd["table_name_"] = table_name

        comm.send_string(sock, json.dumps(cmd))

        msg = comm.log(sock, extra={"cmd": cmd})

        if msg == "Success!":
            if table_name == "" and df_name == "" and not score:
                yhat = comm.recv_float_matrix(sock)
            else:
                yhat = None
        else:
            comm.handle_engine_exception(msg)

        return yhat

    # ----------------------------------------------------------------

    @property
    def accuracy(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the accuracy of the latest scoring run (the
        last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The accuracy of the pipeline.
        """
        return self.scores.accuracy

    # ----------------------------------------------------------------

    @property
    def auc(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the auc of the latest scoring run (the
        last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The auc of the pipeline.
        """
        return self.scores.auc

    # ----------------------------------------------------------------

    def check(
        self,
        population_table: Union[DataFrame, View, data.Subset],
        peripheral_tables: Optional[
            Union[
                Dict[str, Union[DataFrame, View]],
                Sequence[Union[DataFrame, View]],
            ]
        ] = None,
    ) -> Optional[Issues]:
        """
        Checks the validity of the data model.

        Args:
            population_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable.

            peripheral_tables:
                Additional tables corresponding to the ``peripheral``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If passed as a list, the order needs to
                match the order of the corresponding placeholders passed
                to ``peripheral``.

                If you pass a [`Subset`][getml.data.Subset] to `population_table`,
                the peripheral tables from that subset will be used. If you use
                a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
                or [`TimeSeries`][getml.data.TimeSeries], that means you are passing
                a [`Subset`][getml.data.Subset].

        """

        if isinstance(population_table, data.Subset):
            peripheral_tables = population_table.peripheral
            population_table = population_table.population

        peripheral_tables = _transform_peripheral(peripheral_tables, self.peripheral)

        _check_df_types(population_table, peripheral_tables)

        temp = copy.deepcopy(self)

        temp._send()

        cmd: Dict[str, Any] = {}

        cmd["type_"] = temp.type + ".check"
        cmd["name_"] = temp.id

        cmd["peripheral_dfs_"] = [df._getml_deserialize() for df in peripheral_tables]
        cmd["population_df_"] = population_table._getml_deserialize()

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                comm.handle_engine_exception(msg)
            print("Checking data model...")
            msg = comm.log(sock, extra={"cmd": cmd})
            if msg != "Success!":
                comm.handle_engine_exception(msg)
            issues = Issues(comm.recv_issues(sock))
            if len(issues) == 0:
                print("OK.")
            else:
                print(
                    f"The pipeline check generated {len(issues.info)} "
                    + f"issues labeled INFO and {len(issues.warnings)} "
                    + "issues labeled WARNING."
                )

        temp.delete()

        return None if len(issues) == 0 else issues

    # ------------------------------------------------------------

    @property
    def columns(self) -> Columns:
        """
        [`Columns`][getml.pipeline.Columns] object that
        can be used to handle information about the original
        columns utilized by the feature learners.

        Returns:
            The columns object.
        """
        self._check_whether_fitted()
        return Columns(self.id, self.targets, self.peripheral)

    # ----------------------------------------------------------------

    @property
    def cross_entropy(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the cross entropy of the latest scoring
        run (the last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The cross entropy of the pipeline.
        """
        return self.scores.cross_entropy

    # ----------------------------------------------------------------

    def delete(self) -> None:
        """
        Deletes the pipeline from the Engine.

        Warning:
            You can not undo this action!
        """
        self._check_whether_fitted()

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".delete"
        cmd["name_"] = self.id
        cmd["mem_only_"] = False

        comm.send(cmd)

        self._id = NOT_FITTED

    # ------------------------------------------------------------

    def deploy(self, deploy: bool) -> None:
        """Allows a fitted pipeline to be addressable via an HTTP request.
        See [deployment][deployment] for details.

        Args:
            deploy: If `True`, the deployment of the pipeline
                will be triggered.
        """
        self._check_whether_fitted()

        if not isinstance(deploy, bool):
            raise TypeError("'deploy' must be of type bool")

        self._validate()

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".deploy"
        cmd["name_"] = self.id
        cmd["deploy_"] = deploy

        comm.send(cmd)

        self._save()

    # ------------------------------------------------------------

    @property
    def features(self) -> Features:
        """
        [`Features`][getml.pipeline.Features] object that
        can be used to handle the features generated
        by the feature learners.

        Returns:
            The features object.
        """
        self._check_whether_fitted()
        return Features(self.id, self.targets)

    # ------------------------------------------------------------

    def fit(
        self,
        population_table: Union[DataFrame, View, data.Subset],
        peripheral_tables: Optional[
            Union[
                Sequence[Union[DataFrame, View]],
                Dict[str, Union[DataFrame, View]],
            ]
        ] = None,
        validation_table: Optional[Union[DataFrame, View, data.Subset]] = None,
        check: bool = True,
    ) -> Pipeline:
        """Trains the feature learning algorithms, feature selectors
        and predictors.

        Args:
            population_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable.

            peripheral_tables:
                Additional tables corresponding to the ``peripheral``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If passed as a list, the order needs to
                match the order of the corresponding placeholders passed
                to ``peripheral``.

                If you pass a [`Subset`][getml.data.Subset] to `population_table`,
                the peripheral tables from that subset will be used. If you use
                a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
                or [`TimeSeries`][getml.data.TimeSeries], that means you are passing
                a [`Subset`][getml.data.Subset].

            validation_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If you are passing a subset, that subset
                must be derived from the same container as *population_table*.

                Only used for early stopping in [`XGBoostClassifier`][getml.predictors.XGBoostClassifier]
                and [`XGBoostRegressor`][getml.predictors.XGBoostRegressor].

            check:
                Whether you want to check the data model before fitting. The checks are
                equivalent to the checks run by [`check`][getml.Pipeline.check].

        Returns:
            The fitted pipeline.
        """

        additional_tags = (
            ["container-" + population_table.container_id]
            if isinstance(population_table, data.Subset)
            else []
        )

        if (
            isinstance(population_table, data.Subset)
            and isinstance(validation_table, data.Subset)
            and validation_table.container_id != population_table.container_id
        ):
            raise ValueError(
                "The subset used for validation must be from the same container "
                + "as the subset used for training."
            )

        if isinstance(population_table, data.Subset):
            peripheral_tables = population_table.peripheral
            population_table = population_table.population

        if isinstance(validation_table, data.Subset):
            validation_table = validation_table.population

        peripheral_tables = _transform_peripheral(peripheral_tables, self.peripheral)

        _check_df_types(population_table, peripheral_tables)

        if check:
            warnings = self.check(population_table, peripheral_tables)
            if warnings:
                print("To see the issues in full, run .check() on the pipeline.")

        self._send(additional_tags)

        cmd: Dict[str, Any] = {}

        cmd["type_"] = self.type + ".fit"
        cmd["name_"] = self.id

        cmd["peripheral_dfs_"] = [df._getml_deserialize() for df in peripheral_tables]
        cmd["population_df_"] = population_table._getml_deserialize()

        if validation_table is not None:
            cmd["validation_df_"] = validation_table._getml_deserialize()

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)

            if msg != "Found!":
                comm.handle_engine_exception(msg)

            begin = time.monotonic()

            msg = comm.log(sock, extra={"cmd": cmd})

            end = time.monotonic()

            if "Trained" in msg:
                print(msg)
                _print_time_taken(begin, end, "Time taken: ")
            else:
                comm.handle_engine_exception(msg)

        self._save()

        return self.refresh()

    # ------------------------------------------------------------

    @property
    def fitted(self) -> bool:
        """
        Whether the pipeline has already been fitted.

        Returns:
            Whether the pipeline has already been fitted.
        """
        return self._id != NOT_FITTED

    # ----------------------------------------------------------------

    @property
    def mae(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the mae of the latest scoring run (the
        last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The mae of the pipeline.
        """
        return self.scores.mae

    # ------------------------------------------------------------

    @property
    def plots(self) -> Plots:
        """
        [`Plots`][getml.pipeline.Plots] object that
        can be used to generate plots like an ROC
        curve or a lift curve.

        Returns:
            The plots object.
        """
        self._check_whether_fitted()
        return Plots(self.id)

    # ------------------------------------------------------------

    @property
    def id(self) -> str:
        """
        ID of the pipeline. This is used to uniquely identify
        the pipeline on the Engine.

        Returns:
            The ID of the pipeline.
        """
        return self._id

    # ------------------------------------------------------------

    @property
    def is_classification(self) -> bool:
        """
        Whether the pipeline can be used for classification problems.

        Returns:
            Whether the pipeline can be used for classification problems.
        """
        return self._check_classification_or_regression()

    # ------------------------------------------------------------

    @property
    def is_regression(self) -> bool:
        """
        Whether the pipeline can be used for regression problems.

        Returns:
            Whether the pipeline can be used for regression problems.
        """
        return not self.is_classification

    # ------------------------------------------------------------

    @property
    def metadata(self) -> Optional[AllMetadata]:
        """
        Contains information on the data frames
        that were passed to .fit(...). The roles
        contained therein can be directly passed
        to existing data frames to correctly reassign
        the roles of existing columns. If the pipeline
        has not been fitted, this is None.

        Returns:
            The metadata of the pipeline.
        """
        return self._metadata

    # ------------------------------------------------------------

    @property
    def name(self) -> str:
        """
        Returns the ID of the pipeline. The name property is
        kept for backward compatibility.

        Returns:
            The ID of the pipeline.
        """
        return self._id

    # ------------------------------------------------------------

    def predict(
        self,
        population_table: Union[DataFrame, View, data.Subset],
        peripheral_tables: Optional[
            Union[
                Sequence[Union[DataFrame, View]],
                Dict[str, Union[DataFrame, View]],
            ]
        ] = None,
        table_name: str = "",
    ) -> Union[NDArray[np.float_], None]:
        """Forecasts on new, unseen data using the trained ``predictor``.

        Returns the predictions generated by the pipeline based on
        `population_table` and `peripheral_tables` or writes them into
        a database named `table_name`.

        Args:
            population_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable.

            peripheral_tables:
                Additional tables corresponding to the ``peripheral``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If passed as a list, the order needs to
                match the order of the corresponding placeholders passed
                to ``peripheral``.

                If you pass a [`Subset`][getml.data.Subset] to `population_table`,
                the peripheral tables from that subset will be used. If you use
                a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
                or [`TimeSeries`][getml.data.TimeSeries], that means you are passing
                a [`Subset`][getml.data.Subset].

            table_name:
                If not an empty string, the resulting predictions will
                be written into a table in a [`database`][getml.database].
                Refer to [Unified import interface][importing-data-unified-interface] for further information.

        Returns:
            Resulting predictions provided in an array of the (number of rows in `population_table`, number of targets in `population_table`).

        Note:
            Only fitted pipelines
            ([`fit`][getml.Pipeline.fit]) can be used for
            prediction.


        """

        self._check_whether_fitted()

        if isinstance(population_table, data.Subset):
            peripheral_tables = population_table.peripheral
            population_table = population_table.population

        peripheral_tables = _transform_peripheral(peripheral_tables, self.peripheral)

        _check_df_types(population_table, peripheral_tables)

        if not isinstance(table_name, str):
            raise TypeError("'table_name' must be of type str")

        self._validate()

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".transform"
        cmd["name_"] = self.id
        cmd["http_request_"] = False

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                comm.handle_engine_exception(msg)
            y_hat = self._transform(
                peripheral_tables,
                population_table,
                sock,
                predict=True,
                table_name=table_name,
            )

        return y_hat

    # ------------------------------------------------------------

    def refresh(self) -> Pipeline:
        """Reloads the pipeline from the Engine.

        This discards all local changes you have made since the
        last time you called [`fit`][getml.Pipeline.fit].

        Returns:
                Current instance
        """

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".refresh"
        cmd["name_"] = self.id

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)

        if msg[0] != "{":
            comm.handle_engine_exception(msg)

        json_obj = json.loads(msg)

        self._parse_json_obj(json_obj)

        return self

    # ----------------------------------------------------------------

    @property
    def rmse(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the rmse of the latest scoring run
        (the last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The rmse of the pipeline.
        """
        return self.scores.rmse

    # ----------------------------------------------------------------

    @property
    def rsquared(self) -> Union[float, List[float]]:
        """
        A convenience wrapper to retrieve the rsquared of the latest scoring run
        (the last time `.score()` was called) on the pipeline.

        For programmatic access use [`metrics`][getml.pipeline.metrics].

        Returns:
            The rsquared of the pipeline.
        """
        return self.scores.rsquared

    # ----------------------------------------------------------------

    def score(
        self,
        population_table: Union[DataFrame, View, data.Subset],
        peripheral_tables: Optional[
            Union[
                Sequence[Union[DataFrame, View]],
                Dict[str, Union[DataFrame, View]],
            ]
        ] = None,
    ) -> Scores:
        """Calculates the performance of the ``predictor``.

        Returns different scores calculated on `population_table` and
        `peripheral_tables`.

        Args:
            population_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable.

            peripheral_tables:
                Additional tables corresponding to the ``peripheral``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If passed as a list, the order needs to
                match the order of the corresponding placeholders passed
                to ``peripheral``.

                If you pass a [`Subset`][getml.data.Subset] to `population_table`,
                the peripheral tables from that subset will be used. If you use
                a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
                or [`TimeSeries`][getml.data.TimeSeries], that means you are passing
                a [`Subset`][getml.data.Subset].

        Returns:
            The scores of the pipeline.

        Note:
            Only fitted pipelines
            ([`fit`][getml.Pipeline.fit]) can be
            scored.

        """

        self._check_whether_fitted()

        if isinstance(population_table, data.Subset):
            peripheral_tables = population_table.peripheral
            population_table = population_table.population

        peripheral_tables = _transform_peripheral(peripheral_tables, self.peripheral)

        _check_df_types(population_table, peripheral_tables)

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".transform"
        cmd["name_"] = self.id
        cmd["http_request_"] = False

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)

            if msg != "Found!":
                comm.handle_engine_exception(msg)

            self._transform(
                peripheral_tables, population_table, sock, predict=True, score=True
            )

            msg = comm.recv_string(sock)

            if msg != "Success!":
                comm.handle_engine_exception(msg)

            scores = comm.recv_string(sock)

            scores = json.loads(scores)

        self.refresh()

        self._save()

        return self.scores

    # ----------------------------------------------------------------

    @property
    def scores(self) -> Scores:
        """
        Contains all scores generated by [`score`][getml.Pipeline.score]

        Returns:
                A container that holds the scores for the pipeline.

        """
        self._check_whether_fitted()

        scores = self._make_score_history()

        latest = {score: self._get_latest_score(score) for score in _all_metrics}

        return Scores(scores, latest)

    # ----------------------------------------------------------------

    @property
    def scored(self) -> bool:
        """
        Whether the pipeline has been scored.

        Returns:
            Whether the pipeline has been scored.
        """
        if self._scores is None:
            return False
        return len(self._scores) > 1

    # ----------------------------------------------------------------

    @property
    def tables(self) -> Tables:
        """
        [`Tables`][getml.pipeline.Tables] object that
        can be used to handle information about the original
        tables utilized by the feature learners.

        Returns:
            The tables object.
        """
        self._check_whether_fitted()
        return Tables(self.targets, self.columns)

    # ----------------------------------------------------------------

    @property
    def targets(self) -> List[str]:
        """
        Contains the names of the targets used for this pipeline.

        Returns:
            The names of the targets.
        """
        self._check_whether_fitted()
        return copy.deepcopy(self._targets)

    # ----------------------------------------------------------------

    def transform(
        self,
        population_table: Union[DataFrame, View, data.Subset],
        peripheral_tables: Optional[
            Union[
                Sequence[Union[DataFrame, View]],
                Dict[str, Union[DataFrame, View]],
            ]
        ] = None,
        df_name: str = "",
        table_name: str = "",
    ) -> Union[DataFrame, NDArray[np.float_], None]:
        """Translates new data into the trained features.

        Transforms the data passed in `population_table` and
        `peripheral_tables` into features, which can be inserted into
        machine learning models.


        Args:
            population_table:
                Main table containing the target variable(s) and
                corresponding to the ``population``
                [`Placeholder`][getml.data.Placeholder] instance
                variable.

            peripheral_tables:
                Additional tables corresponding to the ``peripheral``
                [`Placeholder`][getml.data.Placeholder] instance
                variable. If passed as a list, the order needs to
                match the order of the corresponding placeholders passed
                to ``peripheral``.

                If you pass a [`Subset`][getml.data.Subset] to `population_table`,
                the peripheral tables from that subset will be used. If you use
                a [`Container`][getml.data.Container], [`StarSchema`][getml.data.StarSchema]
                or [`TimeSeries`][getml.data.TimeSeries], that means you are passing
                a [`Subset`][getml.data.Subset].

            df_name:
                If not an empty string, the resulting features will be
                written into a newly created DataFrame.

            table_name:
                If not an empty string, the resulting features will
                be written into a table in a [`database`][getml.database].
                Refer to [Unified import interface][importing-data-unified-interface] for further information.

        Returns:
            The features generated by the pipeline.

        ??? example
            By default, `transform` returns a [`ndarray`](https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html):
            ```python
            my_features_array = pipe.transform()
            ```
            You can also export your features as a [`DataFrame`][getml.DataFrame]
            by providing the `df_name` argument:
            ```python
            my_features_df = pipe.transform(df_name="my_features")
            ```
            Or you can write the results directly into a database:
            ```python
            getml.database.connect_odbc(...)
            pipe.transform(table_name="MY_FEATURES")
            ```

        Note:
            Only fitted pipelines
            ([`fit`][getml.Pipeline.fit]) can transform
            data into features.

        """

        self._check_whether_fitted()

        if isinstance(population_table, data.Subset):
            peripheral_tables = population_table.peripheral
            population_table = population_table.population

        peripheral_tables = _transform_peripheral(peripheral_tables, self.peripheral)

        _check_df_types(population_table, peripheral_tables)

        self._validate()

        cmd: Dict[str, Any] = {}
        cmd["type_"] = self.type + ".transform"
        cmd["name_"] = self.id
        cmd["http_request_"] = False

        with comm.send_and_get_socket(cmd) as sock:
            msg = comm.recv_string(sock)
            if msg != "Found!":
                comm.handle_engine_exception(msg)
            y_hat = self._transform(
                peripheral_tables,
                population_table,
                sock,
                df_name=df_name,
                table_name=table_name,
            )

        if df_name != "":
            return data.DataFrame(name=df_name).refresh()

        return y_hat

    # ----------------------------------------------------------------

    def _validate(self) -> None:
        if not isinstance(self.id, str):
            raise TypeError("'name' must be of type str")

        if not isinstance(self.data_model, DataModel):
            raise TypeError("'data_model' must be a getml.data.DataModel.")

        if not _is_typed_list(self.peripheral, data.Placeholder):
            raise TypeError(
                "'peripheral' must be either a getml.data.Placeholder or a list thereof"
            )

        if not _is_subclass_list(self.preprocessors, _Preprocessor):
            raise TypeError("'preprocessor' must be a list of _Preprocessor.")

        if not _is_subclass_list(self.feature_learners, _FeatureLearner):
            raise TypeError("'feature_learners' must be a list of _FeatureLearners.")

        if not _is_subclass_list(self.feature_selectors, _Predictor):
            raise TypeError(
                "'feature_selectors' must be a list of getml.predictors._Predictors."
            )

        if not _is_subclass_list(self.predictors, _Predictor):
            raise TypeError(
                "'predictors' must be a list of getml.predictors._Predictors."
            )

        if not isinstance(self.include_categorical, bool):
            raise TypeError("'include_categorical' must be a bool!")

        if not isinstance(self.share_selected_features, numbers.Real):
            raise TypeError("'share_selected_features' must be number!")

        if not _is_typed_list(self.tags, str):
            raise TypeError("'tags' must be a list of str.")

        if self.type != "Pipeline":
            raise ValueError("'type' must be 'Pipeline'")

        for kkey in self.__dict__:
            if kkey not in Pipeline._supported_params:  # pylint: disable=E1101
                raise KeyError(
                    """Instance variable ["""
                    + kkey
                    + """]
                       is not supported in Pipeline."""
                )

        for elem in self.feature_learners:
            elem.validate()

        for elem in self.feature_selectors:
            elem.validate()

        for elem in self.predictors:
            elem.validate()

        self._check_classification_or_regression()
