# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Emitters for the event system.
"""

from __future__ import annotations

from types import TracebackType
from typing import Optional, Sequence, Type

from getml.events.types import Event, EventDispatcher


class DispatcherEventEmitter:
    """
    Emits events to a dispatcher.
    """

    def __init__(
        self,
        dispatcher: EventDispatcher,
    ):
        self.dispatcher = dispatcher

    def __enter__(self):
        self.dispatcher.__enter__()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ):
        self.dispatcher.__exit__(exc_type, exc_value, traceback)

    def emit(self, events: Sequence[Event]):
        self.dispatcher.dispatch(events)
