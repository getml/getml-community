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
    EventHandlerRegistry.unregister_handler(EventSource.ENGINE, track_event)
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

    # Account for the extra COMMAND_FINISHED event added by the listener
    num_handlers = sum(
        len(handlers) for handlers in EventHandlerRegistry.handlers.values()
    )
    assert len(tracked_events) == (len(parsed_events) + 1) * num_handlers

    # Check that we have the expected event types plus COMMAND_FINISHED
    expected_types = set(type for type in EventType if type.name.startswith(prefix))
    expected_types.add(EventType.COMMAND_FINISHED)
    assert set(event.type for event in tracked_events) == expected_types


@pytest.fixture
def failing_event_handler():
    orginal_handlers = EventHandlerRegistry.handlers
    EventHandlerRegistry.handlers = {source: [] for source in EventSource}

    @engine_event_handler
    def failing_handler(event: Event):
        raise Exception("This is a test exception.")

    yield failing_handler
    EventHandlerRegistry.unregister_handler(EventSource.ENGINE, failing_handler)
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


def test_sync_handler_execution():
    """Test that handlers with sync=True execute synchronously in the calling thread."""
    import threading

    orginal_handlers = EventHandlerRegistry.handlers
    EventHandlerRegistry.handlers = {source: [] for source in EventSource}

    sync_thread_id = None
    async_thread_id = None
    main_thread_id = threading.current_thread().ident

    @engine_event_handler(sync=True)
    def sync_handler(event: Event):
        nonlocal sync_thread_id
        sync_thread_id = threading.current_thread().ident

    @engine_event_handler(sync=False)
    def async_handler(event: Event):
        nonlocal async_thread_id
        async_thread_id = threading.current_thread().ident

    event = Event(
        type=EventType.PIPELINE_FIT_STAGE_START,
        source=EventSource.ENGINE,
        attributes={},
    )

    dispatcher = PoolEventDispatcher()
    emitter = DispatcherEventEmitter(dispatcher)

    with dispatcher:
        emitter.emit([event])

    # Sync handler should execute in main thread
    assert sync_thread_id == main_thread_id

    # Async handler should execute in a worker thread
    assert async_thread_id != main_thread_id

    EventHandlerRegistry.unregister_handler(EventSource.ENGINE, sync_handler)
    EventHandlerRegistry.unregister_handler(EventSource.ENGINE, async_handler)
    EventHandlerRegistry.handlers = orginal_handlers
