# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from __future__ import annotations

import re
from dataclasses import dataclass, field, replace
from datetime import datetime
from enum import Enum
from types import TracebackType
from typing import Any, Dict, Literal, Optional, Protocol, Sequence, Type


class Shutdown:
    """
    Sentinel object to signal that a queue should shut down.
    """

    ...


class EventTypeState(str, Enum):
    START = "start"
    PROGRESS = "progress"
    END = "end"


class EventType(str, Enum):
    PIPELINE_FIT_STAGE_START = "pipeline.fit.stage.start"
    PIPELINE_FIT_STAGE_PROGRESS = "pipeline.fit.stage.progress"
    PIPELINE_FIT_STAGE_END = "pipeline.fit.stage.end"
    PIPELINE_FIT_CHECK_START = "pipeline.fit.check.start"
    PIPELINE_FIT_CHECK_PROGRESS = "pipeline.fit.check.progress"
    PIPELINE_FIT_CHECK_END = "pipeline.fit.check.end"
    PIPELINE_FIT_PREPROCESS_START = "pipeline.fit.preprocess.start"
    PIPELINE_FIT_PREPROCESS_PROGRESS = "pipeline.fit.preprocess.progress"
    PIPELINE_FIT_PREPROCESS_END = "pipeline.fit.preprocess.end"
    PIPELINE_FIT_FEATURE_LEARNER_TRAIN_START = (
        "pipeline.fit.feature_learner.train.start"
    )
    PIPELINE_FIT_FEATURE_LEARNER_TRAIN_PROGRESS = (
        "pipeline.fit.feature_learner.train.progress"
    )
    PIPELINE_FIT_FEATURE_LEARNER_TRAIN_END = "pipeline.fit.feature_learner.train.end"
    PIPELINE_FIT_FEATURE_LEARNER_BUILD_START = (
        "pipeline.fit.feature_learner.build.start"
    )
    PIPELINE_FIT_FEATURE_LEARNER_BUILD_PROGRESS = (
        "pipeline.fit.feature_learner.build.progress"
    )
    PIPELINE_FIT_FEATURE_LEARNER_BUILD_END = "pipeline.fit.feature_learner.build.end"
    PIPELINE_FIT_PREDICTOR_TRAIN_START = "pipeline.fit.predictor.train.start"
    PIPELINE_FIT_PREDICTOR_TRAIN_PROGRESS = "pipeline.fit.predictor.train.progress"
    PIPELINE_FIT_PREDICTOR_TRAIN_END = "pipeline.fit.predictor.train.end"
    PIPELINE_TRANSFORM_STAGE_START = "pipeline.transform.stage.start"
    PIPELINE_TRANSFORM_STAGE_PROGRESS = "pipeline.transform.stage.progress"
    PIPELINE_TRANSFORM_STAGE_END = "pipeline.transform.stage.end"
    PIPELINE_TRANSFORM_PREPROCESS_START = "pipeline.transform.preprocess.start"
    PIPELINE_TRANSFORM_PREPROCESS_PROGRESS = "pipeline.transform.preprocess.progress"
    PIPELINE_TRANSFORM_PREPROCESS_END = "pipeline.transform.preprocess.end"
    HYPEROPT_TUNE_FEATURE_LEARNER_START = "hyperopt.tune.feature_learner.start"
    HYPEROPT_TUNE_FEATURE_LEARNER_PROGRESS = "hyperopt.tune.feature_learner.progress"
    HYPEROPT_TUNE_FEATURE_LEARNER_END = "hyperopt.tune.feature_learner.end"
    HYPEROPT_TUNE_PREDICTOR_START = "hyperopt.tune.predictor.start"
    HYPEROPT_TUNE_PREDICTOR_PROGRESS = "hyperopt.tune.predictor.progress"
    HYPEROPT_TUNE_PREDICTOR_END = "hyperopt.tune.predictor.end"
    SETPROJECT_LOADING_PIPELINES_START = "setproject.loading_pipelines.start"
    SETPROJECT_LOADING_PIPELINES_PROGRESS = "setproject.loading_pipelines.progress"
    SETPROJECT_LOADING_PIPELINES_END = "setproject.loading_pipelines.end"
    SETPROJECT_LOADING_HYPEROPTS_START = "setproject.loading_hyperopts.start"
    SETPROJECT_LOADING_HYPEROPTS_PROGRESS = "setproject.loading_hyperopts.progress"
    SETPROJECT_LOADING_HYPEROPTS_END = "setproject.loading_hyperopts.end"
    LOADPROJECTBUNDLE_LOADING_PROJECT_START = "loadprojectbundle.loading_project.start"
    LOADPROJECTBUNDLE_LOADING_PROJECT_PROGRESS = (
        "loadprojectbundle.loading_project.progress"
    )
    LOADPROJECTBUNDLE_LOADING_PROJECT_END = "loadprojectbundle.loading_project.end"

    def progress(self) -> EventType:
        """
        Progress (transition) event type to the succeeding state.

        If the event type is already in the end state, no transition is
        performed and the end state is returned.
        """
        if self.value.endswith(".start"):
            return EventType(self.value.replace(".start", ".progress"))
        if self.value.endswith(".progress"):
            return EventType(self.value.replace(".progress", ".end"))
        return self

    @property
    def state(self) -> EventTypeState:
        """
        The state of the event type.
        """
        if self.value.endswith(".start"):
            return EventTypeState.START
        if self.value.endswith(".progress"):
            return EventTypeState.PROGRESS
        return EventTypeState.END

    def transition_to(self, state: EventTypeState) -> EventType:
        """
        Return the event type with the state suffix.
        """
        return EventType(re.sub(r"\.\w+$", f".{state.value}", self.value))


class EventSource(str, Enum):
    ENGINE = "engine"
    MONITOR = "monitor"


@dataclass
class EventContext:
    cmd: Dict[str, Any]
    source: EventSource


@dataclass
class Event:
    source: EventSource
    type: EventType
    attributes: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.now)
    _version: Literal[1] = 1

    def progress(self) -> Event:
        """
        Progress (transition) event to the succeeding state.

        If the event is already in the end state, no transition is
        performed and the end state is returned.
        """
        return replace(self, type=self.type.progress())


class EventParser(Protocol):
    def parse(self, message: str) -> Sequence[Event]: ...


class EventDispatcher(Protocol):
    def __enter__(self) -> EventDispatcher: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ): ...
    def start(self): ...
    def stop(self, wait: bool = True): ...
    def dispatch(self, events: Sequence[Event]): ...


class EventEmitter(Protocol):
    dispatcher: EventDispatcher

    def __enter__(self) -> EventEmitter: ...
    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ): ...
    def emit(self, events: Sequence[Event]): ...
