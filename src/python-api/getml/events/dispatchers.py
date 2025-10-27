# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Dispatchers for events.
"""

from __future__ import annotations

import queue
import warnings
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from types import TracebackType
from typing import Dict, Optional, Sequence, Type, Union

from getml.events.handlers import EventHandlerRegistry, HandlerMetadata
from getml.events.types import Event, EventSource, Shutdown


class PoolEventDispatcher:
    """
    Receives events and dispatches them to registered event handlers inside an executor
    managed pool.

    Creates one queue per registered event handler and one thread per queue inside the
    pool. The dispatcher manages the lifecycle of the pool and the queues.

    The number of threads/processes spawned is equal to the number of handlers
    registered over all event sources.
    """

    def __init__(
        self,
        executor: Optional[Executor] = None,
    ):
        self.queues = {
            source: {
                metadata: queue.Queue()
                for metadata in EventHandlerRegistry.handlers[source]
            }
            for source in EventSource
        }

        if not executor:
            executor = ThreadPoolExecutor()
        self.executor = executor

        self.futures: Dict[HandlerMetadata, Future] = {}

    def __enter__(self):
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        if exc_type:
            self.stop(wait=False)

        self.stop()

    def dispatch(self, events: Sequence[Event]):
        for event in events:
            for metadata, handler_queue in self.queues[event.source].items():
                if metadata.sync:
                    # Execute synchronously in the calling thread
                    try:
                        metadata.handler(event)  # type: ignore
                    except Exception as e:
                        warnings.warn(
                            f"An exception occurred while dispatching event {event} to handler {metadata.handler}: {e}",
                            RuntimeWarning,
                        )
                else:
                    # Dispatch to worker thread via queue
                    handler_queue.put(event)

    def process(
        self,
        metadata: HandlerMetadata,
        handler_queue: queue.Queue[Union[Event, Shutdown]],
    ):
        while True:
            try:
                event = handler_queue.get(timeout=1)
            except queue.Empty:
                continue

            if event is Shutdown:
                handler_queue.task_done()
                return

            try:
                metadata.handler(event)  # type: ignore
            except Exception as e:
                warnings.warn(
                    f"An exception occurred while dispatching event {event} to handler {metadata.handler}: {e}",
                    RuntimeWarning,
                )
            finally:
                handler_queue.task_done()

    def start(self):
        for source in self.queues:
            for metadata, handler_queue in self.queues[source].items():
                if not metadata.sync:
                    # Only submit async handlers to the thread pool
                    self.futures[metadata] = self.executor.submit(
                        self.process, metadata, handler_queue
                    )

    def stop(self, wait: bool = True):
        if not wait:
            self.executor.shutdown(wait=False)
            return

        for source in self.queues:
            for metadata, handler_queue in self.queues[source].items():
                if not metadata.sync:
                    # Only wait for and shutdown async handlers
                    handler_queue.join()
                    handler_queue.put(Shutdown)
        self.executor.shutdown(wait=True)
