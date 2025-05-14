# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Parsers for creating structured events from raw log messages.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union

from getml.events.regex import (
    LOG_MESSAGE_REGEX,
    UNSPECIFIED_PROGRESS_MESSAGE_REGEX,
    EventMessageRegex,
)
from getml.events.types import (
    Event,
    EventContext,
    EventSource,
    EventType,
    EventTypeState,
)


def _coerce_attribute_values(attributes: Dict[str, str]) -> Dict[str, Union[str, int]]:
    """
    Coerce attribute values to the correct types.
    """
    coerced_attributes = {}
    for key, value in attributes.items():
        if key in {"progress", "n_features", "rows", "tree"} and value is not None:
            coerced_attributes[key] = int(value)
        else:
            coerced_attributes[key] = value
    return coerced_attributes


def _process_attributes(
    attributes: Dict[str, Union[str, str]],
) -> Dict[str, Union[str, int]]:
    processed_attributes = {}
    for key, value in attributes.items():
        if key == "type":
            processed_attributes[key] = value.replace(" ", "_")
        else:
            processed_attributes[key] = value
    return _coerce_attribute_values(processed_attributes)


def _create_attributes(
    groupdict: Dict[str, str],
    context: EventContext,
    body: Optional[str] = None,
) -> Dict[str, Union[str, int]]:
    """
    Create attributes from a match group dict, the context and the body of the
    log message.
    """
    attributes = {**groupdict}

    if "id_" in context.cmd:
        attributes["id"] = context.cmd["id_"]

    if body:
        description, *_ = body.split(" Progress: ")
        attributes["description"] = description

    return _process_attributes(attributes)


class LogMessageEventParser:
    """
    Parses raw log messages from engine and monitor into zero, one or multiple
    structured events.

    The parser is stateful because progress messages can be ambiguous
    without context.
    """

    def __init__(self, context: EventContext):
        # The context of the message that is being parsed. Contains the
        # command that was sent to either the engine or the monitor and
        # the source of the event (engine or monitor).
        self.context = context
        # The parser needs to keep track of the current event because
        # unspecified progress messages are ambiguous.
        # Also, there are ambiguous progress messages for feature learners
        # and predictors.
        self.current_event: Optional[Event] = None

    def parse(self, message: str) -> List[Event]:
        """
        Parse a log message into an event.
        """
        events = []

        if not (log_match := LOG_MESSAGE_REGEX.match(message)):
            return []

        body = log_match.group("body")

        if progress_event := self._try_parse_unspecified_progress_event(body):
            events.append(progress_event)
        elif typed_event := self._try_parse_typed_event(body):
            events.append(typed_event)

        if end_event := self._try_create_end_event():
            events.append(end_event)

        return events

    def _try_parse_unspecified_progress_event(self, body: str) -> Optional[Event]:
        """
        Process an unspecified progress message.
        """
        if progress_match := UNSPECIFIED_PROGRESS_MESSAGE_REGEX.match(body):
            attributes = _create_attributes(progress_match.groupdict(), self.context)
            if self.current_event:
                event = Event(
                    source=EventSource.ENGINE,
                    type=self.current_event.type.transition_to(EventTypeState.PROGRESS),
                    attributes={**self.current_event.attributes, **attributes},
                )
                self.current_event = event
                return event

    def _try_reattach_model(self, event: Event) -> Event:
        """
        Re-attach model to progressed feature learner events.
        """
        if self.current_event and event.type in {
            EventType.PIPELINE_FIT_FEATURE_LEARNER_TRAIN_PROGRESS,
            EventType.PIPELINE_FIT_FEATURE_LEARNER_BUILD_PROGRESS,
        }:
            event.attributes["model"] = self.current_event.attributes["model"]
        return event

    def _try_reattach_type(self, event: Event) -> Event:
        """
        Re-attach type to progressed predictor events.
        """
        if self.current_event and event.type in {
            EventType.PIPELINE_FIT_PREDICTOR_TRAIN_PROGRESS,
        }:
            event.attributes["type"] = self.current_event.attributes["type"]
        return event

    def _try_parse_typed_event(self, body: str) -> Optional[Event]:
        """
        Process a specific event, a match maps to a specific event type.
        """
        prefix = self.context.cmd.get("type_", "").replace(".", "_").upper()

        for event_type, regex in EventMessageRegex.with_prefix(prefix):
            if event_match := regex.match(body):
                attributes = _create_attributes(
                    event_match.groupdict(), self.context, body
                )

                event = Event(
                    source=self.context.source,
                    type=event_type,
                    attributes=attributes,
                )
                event = self._try_reattach_model(event)
                event = self._try_reattach_type(event)
                self.current_event = event
                return event

    def _try_create_end_event(self) -> Optional[Event]:
        """
        Add an end event if the current event is a progress event that has
        reached the end state (100%).
        """
        if (
            (event := self.current_event)
            and event.type.state == EventTypeState.PROGRESS
            and event.attributes.get("progress") == 100
        ):
            end_event = event.progress()
            self.current_event = end_event
            return end_event
