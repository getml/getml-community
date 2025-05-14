# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Event system.
"""

from typing import Callable, Dict, List

from getml.events.types import Event, EventSource

EventHandler = Callable[[Event], None]


class EventHandlerRegistry:
    handlers: Dict[EventSource, List[EventHandler]] = {}

    def __init__(self, source: EventSource):
        self.source = source
        type(self).handlers[source] = []

    def __call__(self, handler: EventHandler) -> EventHandler:
        """
        Register an event handler for the source associated with the instance.
        """
        self.register_handler(self.source, handler)
        return handler

    @classmethod
    def register_handler(cls, source: EventSource, handler: EventHandler):
        """
        Register an event handler for a specific source.
        """
        cls.handlers[source].append(handler)


engine_event_handler = EventHandlerRegistry(EventSource.ENGINE)
monitor_event_handler = EventHandlerRegistry(EventSource.MONITOR)
