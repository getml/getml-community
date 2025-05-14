# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Regex patterns for parsing engine messages.
"""

from __future__ import annotations

import re
from enum import Enum
from typing import Iterator, Tuple

from getml.events.types import EventType

LOG_MESSAGE_REGEX = re.compile(r"log: (?P<body>.*)")
UNSPECIFIED_PROGRESS_MESSAGE_REGEX = re.compile(r"Progress: (?P<progress>\d+)%.*")


_PIPELINE_STAGE_START = re.compile(r"Staging...")
_PIPELINE_CHECK_START = re.compile(r"Checking...")
_PIPELINE_PREPROCESS_START = re.compile(r"Preprocessing...")


class EventMessageRegex(Enum):
    PIPELINE_FIT_STAGE_START = _PIPELINE_STAGE_START
    PIPELINE_FIT_CHECK_START = _PIPELINE_CHECK_START
    PIPELINE_FIT_PREPROCESS_START = _PIPELINE_PREPROCESS_START
    PIPELINE_FIT_FEATURE_LEARNER_TRAIN_START = re.compile(
        r"(?P<model>FastProp|Multirel|Relboost|Fastboost|RelMT): "
        r"(Training features|Trying (?P<n_features>\d+) features...)"
    )
    PIPELINE_FIT_FEATURE_LEARNER_TRAIN_PROGRESS = re.compile(
        r"(Trained (?P<feature>FEATURE_\d+)|Built (?P<n_features>\d+) features). "
        r"Progress: (?P<progress>\d+)%."
    )
    PIPELINE_FIT_FEATURE_LEARNER_BUILD_START = re.compile(
        r"(?P<model>FastProp|Multirel|Relboost|Fastboost|RelMT): "
        r"Building features..."
    )
    PIPELINE_FIT_FEATURE_LEARNER_BUILD_PROGRESS = re.compile(
        r"Built ((?P<feature>FEATURE_\d+)|(?P<n_rows>\d+) rows). "
        r"Progress: (?P<progress>\d+)%."
    )
    PIPELINE_FIT_PREDICTOR_TRAIN_START = re.compile(
        r"(?P<model>XGBoost|ScaleGBM|LinearRegression|LogisticRegression): Training as "
        r"(?P<type>feature selector|predictor)..."
    )
    PIPELINE_FIT_PREDICTOR_TRAIN_PROGRESS = re.compile(
        r"(?P<model>XGBoost|ScaleGBM): Trained tree (?P<tree>\d+). "
        r"Progress: (?P<progress>\d+)%."
    )
    PIPELINE_TRANSFORM_STAGE_START = _PIPELINE_STAGE_START
    PIPELINE_TRANSFORM_PREPROCESS_START = _PIPELINE_PREPROCESS_START
    HYPEROPT_TUNE_FEATURE_LEARNER_START = re.compile(
        r"Tuning (?P<model>FastProp|Multirel|Relboost|Fastboost|RelMT)..."
    )
    HYPEROPT_TUNE_PREDICTOR_START = re.compile(
        r"Tuning (?P<model>XGBoost|ScaleGBM|LinearRegression|LogisticRegression)..."
    )
    SETPROJECT_LOADING_PIPELINES_START = re.compile(r"Loading pipelines...")
    SETPROJECT_LOADING_HYPEROPTS_START = re.compile(r"Loading hyperopts...")
    LOADPROJECTBUNDLE_LOADING_PROJECT_START = re.compile(r"Loading project...")

    @classmethod
    def with_prefix(cls, prefix: str) -> Iterator[Tuple[EventType, re.Pattern]]:
        for name, member in cls.__members__.items():
            if name.startswith(prefix):
                yield EventType[name], member.value
