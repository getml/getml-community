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
from typing import Callable, Dict, Optional, Sequence, Type, Union

from getml.events.handlers import EventHandlerRegistry
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
                handler: queue.Queue()
                for handler in EventHandlerRegistry.handlers[source]
            }
            for source in EventSource
        }

        if not executor:
            executor = ThreadPoolExecutor()
        self.executor = executor

        self.futures: Dict[Callable[[Event], None], Future] = {}

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
            for queue in self.queues[event.source].values():
                queue.put(event)

    def process(
        self,
        handler: Callable[[Event], None],
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
                handler(event)  # type: ignore
            except Exception as e:
                warnings.warn(
                    f"An exception occurred while dispatching event {event} to handler {handler}: {e}",
                    RuntimeWarning,
                )
            finally:
                handler_queue.task_done()

    def start(self):
        for source in self.queues:
            for handler, queue in self.queues[source].items():
                self.futures[handler] = self.executor.submit(
                    self.process, handler, queue
                )

    def stop(self, wait: bool = True):
        if not wait:
            self.executor.shutdown(wait=False)
            return

        for source in self.queues:
            for queue in self.queues[source].values():
                queue.join()
                queue.put(Shutdown)
        self.executor.shutdown(wait=True)
