# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Event system.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, TypeVar, Union, overload

from getml.events.types import Event, EventSource

EventHandler = Callable[[Event], None]
H = TypeVar("H", bound=EventHandler)


@dataclass(frozen=True)
class HandlerMetadata:
    """Metadata for an event handler."""

    handler: EventHandler
    sync: bool = False


class EventHandlerRegistry:
    handlers: Dict[EventSource, List[HandlerMetadata]] = {}

    def __init__(self, source: EventSource):
        self.source = source
        type(self).handlers[source] = []

    @overload
    def __call__(self, handler: H) -> H: ...

    @overload
    def __call__(self, *, sync: bool = False) -> Callable[[H], H]: ...

    def __call__(
        self, handler: Optional[H] = None, *, sync: bool = False
    ) -> Union[Callable[[H], H], H]:
        """
        Register an event handler for the source associated with the instance.

        Can be used as a decorator with or without arguments:
        - @event_handler
        - @event_handler(sync=True)

        Args:
            handler: The event handler function
            sync: If True, handler executes synchronously in the calling thread
                  (no dispatching). If False, handler executes in a worker thread pool.

        Returns:
            The handler function (for decorator usage)
        """
        if handler is None:
            # Called with arguments: @event_handler(sync=...)
            def decorator(func: H) -> H:
                self.register_handler(self.source, func, sync=sync)
                return func

            return decorator

        # Called without arguments: @event_handler
        self.register_handler(self.source, handler, sync=sync)
        return handler

    @classmethod
    def register_handler(
        cls, source: EventSource, handler: EventHandler, sync: bool = False
    ):
        """
        Register an event handler for a specific source.

        Args:
            source: The event source
            handler: The event handler function
            sync: If True, handler executes synchronously in the calling thread
        """
        metadata = HandlerMetadata(handler=handler, sync=sync)
        cls.handlers[source].append(metadata)

    @classmethod
    def unregister_handler(cls, source: EventSource, handler: EventHandler):
        """
        Unregister an event handler for a specific source.

        Args:
            source: The event source
            handler: The event handler function to remove
        """
        cls.handlers[source] = [
            metadata for metadata in cls.handlers[source] if metadata.handler != handler
        ]


engine_event_handler = EventHandlerRegistry(EventSource.ENGINE)
monitor_event_handler = EventHandlerRegistry(EventSource.MONITOR)
