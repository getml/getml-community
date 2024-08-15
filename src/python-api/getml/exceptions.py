# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

from functools import wraps
from typing import Any, Callable, Dict, Optional

import pyarrow as pa
from pyarrow.lib import ArrowInvalid


class ArrowCastExceptionHandlerRegistry:
    """
    A registry for handlers of exceptions originating from the Arrow library.

    Instances of this class can be used as a decorator to register exception
    handlers. Handlers are registered for specific target types.

    The handlers are called with the exception and the field that
    caused the exception.

    As there is only a single Exception type raised by Arrow (`ArrowInvalid`),
    the handlers have to parse the exception themselves.
    """

    handlers: Dict[pa.DataType, Callable[[ArrowInvalid, pa.Field], None]] = {}

    def __call__(
        self,
        target_type: pa.DataType,
    ) -> Callable[
        [Callable[[ArrowInvalid, pa.Field], None]],
        Callable[[pa.Array, pa.DataType], pa.Array],
    ]:
        return self.register(target_type)

    @classmethod
    def register(
        cls, target_type: pa.DataType
    ) -> Callable[
        [Callable[[ArrowInvalid, pa.Field], None]],
        Callable[[pa.Array, pa.DataType], pa.Array],
    ]:
        def decorator(func: Callable[[ArrowInvalid, pa.Field], None]):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            cls.handlers[target_type] = wrapper
            return wrapper

        return decorator

    @classmethod
    def retrieve(
        cls, target_type: pa.DataType
    ) -> Callable[[pa.Array, pa.DataType], pa.Array]:
        if target_type not in cls.handlers:
            raise ValueError(f"No handler registered for type: {target_type}")
        return cls.handlers[target_type]


arrow_cast_exception_handler = ArrowCastExceptionHandlerRegistry()


class EngineExceptionHandlerRegistry:
    """
    A registry for handlers of exceptions originating from the getML Engine.
    Instances of this class can be used as a decorator to register exception
    handlers. As all exceptions are sent as raw strings, the handlers have to
    parse the exceptions themselves.
    """

    handlers = []

    def __call__(
        self, handler: Callable[[str, Dict[str, Any]], None]
    ) -> Callable[[str, Dict[str, Any]], None]:
        return self.register(handler)

    @classmethod
    def register(cls, handler: Callable[[str, Dict[str, Any]], None]):
        cls.handlers.append(handler)
        return handler


engine_exception_handler = EngineExceptionHandlerRegistry()


def handle_engine_exception(msg: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Handles exceptions raised by the getML Engine. Iterates over all
    registered handlers and calls them with the error message.
    Registered handlers parse the message and may raise an exception
    themselves.

    If no registered handler can handle the exception, the message is
    raised inside and OSError as is.

    Args:
        msg: Error message returned by the getML Engine.
    """

    if extra is None:
        extra = {}

    for handler in EngineExceptionHandlerRegistry.handlers:
        handler(msg, extra=extra)

    raise OSError(msg)
