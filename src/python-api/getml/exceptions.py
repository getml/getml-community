# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import re
from inspect import cleandoc
from typing import Any, Callable, Dict, Optional, Protocol, Tuple

import pyarrow as pa
from pyarrow.lib import ArrowInvalid

from getml.constants import ENTERPRISE_DOCS_URL


class ArrowExceptionHandler(Protocol):
    def __call__(
        self, exc: ArrowInvalid, source_array: pa.Array, target_field: pa.Field
    ) -> None: ...


ENTERPRISE_FEATURE_NOT_AVAILABLE_REGEX = re.compile(
    r"The (.*) (.*) is not supported in the community edition. Please upgrade "
    r"to getML enterprise to use this. An overview of what is supported in the "
    r"community edition can be found in the official getML documentation."
)


ENTERPRISE_FEATURE_NOT_AVAILABLE_ERROR_MSG_TEMPLATE = cleandoc(
    "The {missing_feature_name} {missing_feature_type} "
    "is unique to getML Enterprise and is not available "
    "in the getML Community edition you are currently using.\n\n"
    "Please visit {enterprise_docs_url} to learn about getML's "
    "advanced algorithms, extended feature set, commercial plans, and available "
    "trial options. "
)


def _nop_arrow_cast_exception_handler(exc: Exception, field: pa.Field) -> None:
    raise exc


class ArrowCastExceptionHandlerRegistry:
    """
    A registry for handlers of exceptions originating from the Arrow library.

    Instances of this class can be used as a decorator to register exception
    handlers. Handlers are registered for specific target arrow types.

    The handlers are called with the exception (exc) and the field (field) which
    cast caused the exception.

    As there is only a single Exception type raised by Arrow (`ArrowInvalid`),
    the handlers have to parse the exception themselves.
    """

    handlers: Dict[Tuple[pa.DataType, pa.DataType], ArrowExceptionHandler] = {}

    def __call__(
        self,
        source_type: pa.DataType,
        target_type: pa.DataType,
    ) -> Callable[[ArrowExceptionHandler], ArrowExceptionHandler]:
        return self.register(source_type, target_type)

    @classmethod
    def register(
        cls,
        source_type: pa.DataType,
        target_type: pa.DataType,
    ) -> Callable[[ArrowExceptionHandler], ArrowExceptionHandler]:
        def decorator(handler: ArrowExceptionHandler):
            cls.handlers[(source_type, target_type)] = handler
            return handler

        return decorator

    @classmethod
    def retrieve(
        cls, source_type: pa.DataType, target_type: pa.DataType
    ) -> ArrowExceptionHandler:
        if (source_type, target_type) not in cls.handlers:
            return _nop_arrow_cast_exception_handler
        return cls.handlers[(source_type, target_type)]


arrow_cast_exception_handler = ArrowCastExceptionHandlerRegistry()


class EngineExceptionHandlerRegistry:
    """
    A registry for handlers of exceptions originating from the getML Engine.
    Instances of this class can be used as a decorator to register exception
    handlers. As all exceptions are sent as raw strings, the handlers have to
    parse the exceptions themselves.

    The handlers are called with the raw exception string (msg) and an possibly
    empty dictionary of extra information.
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


@engine_exception_handler
def handle_enterprise_feature_not_available_error(msg: str, extra: Dict[str, Any]):
    """
    Handler for the "Enterprise feature not available" error message.
    """
    match = ENTERPRISE_FEATURE_NOT_AVAILABLE_REGEX.match(msg)
    if match:
        missing_feature_name = match.group(1)
        missing_feature_type = match.group(2)
        raise OSError(
            ENTERPRISE_FEATURE_NOT_AVAILABLE_ERROR_MSG_TEMPLATE.format(
                enterprise_docs_url=ENTERPRISE_DOCS_URL,
                missing_feature_name=missing_feature_name,
                missing_feature_type=missing_feature_type,
            )
        )
