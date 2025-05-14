# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import itertools as it

import pytest
from rich import print

from getml.communication import LogStreamListener
from getml.events import (
    DispatcherEventEmitter,
    LogMessageEventParser,
    PoolEventDispatcher,
    engine_event_handler,
)
from getml.events.handlers import EventHandlerRegistry
from getml.events.types import Event, EventSource, EventType


@pytest.fixture
def tracked_events():
    emitted_events = []

    orginal_handlers = EventHandlerRegistry.handlers
    EventHandlerRegistry.handlers = {source: [] for source in EventSource}

    @engine_event_handler
    def track_event(event: Event):
        print(event)
        emitted_events.append(event)

    yield emitted_events
    EventHandlerRegistry.handlers[EventSource.ENGINE].remove(track_event)
    EventHandlerRegistry.handlers = orginal_handlers


def test_log_stream_listener(monkeypatch, pipeline_context, tracked_events):
    monkeypatch.setattr(
        "getml.communication._list_projects_impl", lambda running_only: []
    )
    monkeypatch.setattr("getml.communication.tcp_port", 1709)
    context, socketlike = pipeline_context
    parser = LogMessageEventParser(context=context)
    dispatcher = PoolEventDispatcher()
    emitter = DispatcherEventEmitter(dispatcher)

    parsed_events = list(
        it.chain.from_iterable(parser.parse(msg) for msg in socketlike.messages)
    )

    with LogStreamListener(
        parser=parser,
        emitter=emitter,
    ) as listener:
        listener.listen(socket=socketlike, exit_on=lambda msg: msg == "^D")

    prefix = context.cmd["type_"].replace(".", "_").upper()

    # at least one event for each log message
    assert len(tracked_events) >= len(
        [msg for msg in socketlike.messages if msg.startswith("log: ")]
    )
    assert len(tracked_events) == len(parsed_events) * len(
        [
            handler
            for source in EventSource
            for handler in EventHandlerRegistry.handlers[source]
        ]
    )
    assert set(event.type for event in tracked_events) == set(
        type for type in EventType if type.name.startswith(prefix)
    )


@pytest.fixture
def failing_event_handler():
    orginal_handlers = EventHandlerRegistry.handlers
    EventHandlerRegistry.handlers = {source: [] for source in EventSource}

    @engine_event_handler
    def failing_handler(event: Event):
        raise Exception("This is a test exception.")

    yield failing_handler
    EventHandlerRegistry.handlers[EventSource.ENGINE].remove(failing_handler)
    EventHandlerRegistry.handlers = orginal_handlers


def test_event_handler_exception(failing_event_handler):
    event = Event(
        type=EventType.PIPELINE_FIT_STAGE_START,
        source=EventSource.ENGINE,
        attributes={},
    )
    dispatcher = PoolEventDispatcher()

    emitter = DispatcherEventEmitter(dispatcher)

    with pytest.warns(RuntimeWarning) as warnings:
        with dispatcher:
            emitter.emit([event])

    assert str(warnings[0].message).startswith(
        "An exception occurred while dispatching event"
    )
    assert failing_event_handler.__name__ in str(warnings[0].message)
    assert str(event) in str(warnings[0].message)
