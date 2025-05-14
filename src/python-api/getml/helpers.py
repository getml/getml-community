# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of helper classes that are relevant
for many submodules.
"""

from typing import (
    Collection,
    Iterable,
    Type,
    TypeVar,
    Union,
)

from typing_extensions import TypeGuard  # py3.10

T_co = TypeVar("T_co", covariant=True)


# --------------------------------------------------------------------


def _check_parameter_bounds(parameter, parameter_name, bounds):
    """Checks whether a particular parameter does lie within the provided
    `bounds`.

    Args:
        parameter (numeric): Particular value of an instance variable.
        key_name (string): Name of the parameter used for an
            expressive Exception
        bounds (list[numeric]): Numerical list of length 2
            specifying the lower and upper bound (in that order)
            of `parameter`.
    """
    if parameter < bounds[0] or parameter > bounds[1]:
        raise ValueError(
            "'"
            + parameter_name
            + "' is only defined for range ["
            + str(bounds[0])
            + ", "
            + str(bounds[1])
            + "]"
        )


# --------------------------------------------------------------------


def _is_iterable_not_str(
    obj: Union[T_co, Iterable[T_co]],
) -> TypeGuard[Iterable[T_co]]:
    """
    Check whether an object is an iterable (supports `__iter__`) but not a
    string.
    """
    return isinstance(obj, Iterable) and not isinstance(obj, str)


# --------------------------------------------------------------------


def _is_iterable_not_str_of_type(
    obj: Union[T_co, Iterable[T_co]], type: Type[T_co]
) -> TypeGuard[Iterable[T_co]]:
    """
    Check whether an object is an iterable (supports `__iter__`) but not a
    string and all elements are of type `type`.
    """
    if _is_iterable_not_str(obj):
        return all(isinstance(x, type) for x in obj)
    return False


# --------------------------------------------------------------------


def _is_collection_not_str(
    obj: Union[T_co, Collection[T_co]],
) -> TypeGuard[Collection[T_co]]:
    """
    Check whether an object is a collection (supports `__iter__`, `__len__` and
    `__contains__`) but not a string.
    """
    return isinstance(obj, Collection) and not isinstance(obj, str)


# --------------------------------------------------------------------


def _is_collection_not_str_of_type(
    obj: Union[T_co, Collection[T_co]], type: Type[T_co]
) -> TypeGuard[Collection[T_co]]:
    """
    Check whether an object is a collection (supports `__iter__`, `__len__` and
    `__contains__`) but not a string and all elements are of type `type`.
    """
    if _is_collection_not_str(obj):
        return all(isinstance(x, type) for x in obj)
    return False


# --------------------------------------------------------------------


def _is_nonempty_collection_not_str(
    obj: Union[T_co, Collection[T_co]],
) -> TypeGuard[Collection[T_co]]:
    """
    Check whether an object is a collection (supports `__iter__`, `__len__` and
    `__contains__`) but not a string and is not empty.
    """
    if _is_collection_not_str(obj):
        return len(obj) > 0
    return False
