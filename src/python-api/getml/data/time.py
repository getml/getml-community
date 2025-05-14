# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""Convenience functions for the handling of time stamps.

In getML, time stamps are always expressed as a floating point value. This float
measures the number of seconds since UNIX time (January 1, 1970, 00:00:00).
Smaller units of time are expressed as fractions of a second.

To make this a bit easier to handle, this module contains simple convenience
functions that express other time units in terms of seconds."""

import datetime as dt

# --------------------------------------------------------------------------


def seconds(num: float) -> float:
    """
    Returns the number of seconds.

    Args:
        num:
            The number of seconds.

    Returns:
        *num*
    """
    return num


# --------------------------------------------------------------------------


def minutes(num: float) -> float:
    """
    Expresses *num* minutes in terms of seconds.

    Args:
        num:
            The number of minutes.

    Returns:
        *num* minutes expressed in terms of seconds.
    """
    return seconds(num) * 60.0


# --------------------------------------------------------------------------


def hours(num: float) -> float:
    """
    Expresses *num* hours in terms of seconds.

    Args:
        num:
            The number of hours.

    Returns:
        *num* hours expressed in terms of seconds.
    """
    return minutes(num) * 60.0


# --------------------------------------------------------------------------


def days(num: float) -> float:
    """
    Expresses *num* days in terms of seconds.

    Args:
        num:
            The number of days.

    Returns:
        *num* days expressed in terms of seconds.
    """
    return hours(num) * 24.0


# --------------------------------------------------------------------------


def weeks(num: float) -> float:
    """
    Expresses *num* weeks in terms of seconds.

    Args:
        num:
            The number of weeks.

    Returns:
        *num* weeks expressed in terms of seconds.
    """
    return days(num) * 7.0


# --------------------------------------------------------------------------


def milliseconds(num: float) -> float:
    """
    Expresses *num* milliseconds in terms of fractions of a second.

    Args:
        num:
            The number of milliseconds.

    Returns:
        *num* milliseconds expressed in terms of seconds.
    """
    return seconds(num) / 1000.0


# --------------------------------------------------------------------------


def microseconds(num: float) -> float:
    """
    Expresses *num* microseconds in terms of fractions of a second.

    Args:
        num:
            The number of microseconds.

    Returns:
        *num* microseconds expressed in terms of seconds.
    """
    return milliseconds(num) / 1000.0


# --------------------------------------------------------------------------


def datetime(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
    second: int = 0,
    microsecond: int = 0,
) -> float:
    """
    Returns the number of seconds since UNIX time (January 1, 1970, 00:00:00).

    Args:
        year:
            Year component of the date.

        month:
            Month component of the date.

        day:
            Day component of the date.

        hour:
            Hour component of the date.

        minute:
            Minute component of the date.

        second:
            Second component of the date.

        microsecond:
            Microsecond component of the date.

    Returns:
        The number of seconds since UNIX time (January 1, 1970, 00:00:00).
    """
    return dt.datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        microsecond,
        tzinfo=dt.timezone.utc,
    ).timestamp()


# --------------------------------------------------------------------------

__all__ = (
    "seconds",
    "minutes",
    "hours",
    "days",
    "weeks",
    "milliseconds",
    "microseconds",
    "datetime",
)
