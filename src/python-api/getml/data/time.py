# Copyright 2022 The SQLNet Company GmbH
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

import numpy as np


# --------------------------------------------------------------------------


def seconds(num):
    """
    Transforms *num* into a float64.

    Args:
        num (float):
            The number of seconds.

    Returns:
        float: *num* as a float64.
    """
    return np.float64(num)


# --------------------------------------------------------------------------


def minutes(num):
    """
    Expresses *num* minutes in terms of seconds.

    Args:
        num (float):
            The number of minutes.

    Returns:
        float: *num* minutes expressed in terms of seconds.
    """
    return seconds(num) * 60.0


# --------------------------------------------------------------------------


def hours(num):
    """
    Expresses *num* hours in terms of seconds.

    Args:
        num (float):
            The number of hours.

    Returns:
        float: *num* hours expressed in terms of seconds.
    """
    return minutes(num) * 60.0


# --------------------------------------------------------------------------


def days(num):
    """
    Expresses *num* days in terms of seconds.

    Args:
        num(float):
            The number of days.

    Returns:
        float: *num* days expressed in terms of seconds.
    """
    return hours(num) * 24.0


# --------------------------------------------------------------------------


def weeks(num):
    """
    Expresses *num* weeks in terms of seconds.

    Args:
        num (float):
            The number of weeks.

    Returns:
        float: *num* weeks expressed in terms of seconds.
    """
    return days(num) * 7.0


# --------------------------------------------------------------------------


def milliseconds(num):
    """
    Expresses *num* milliseconds in terms of fractions of a second.

    Args:
        num (float):
            The number of milliseconds.

    Returns:
        float: *num* milliseconds expressed in terms of seconds.
    """
    return seconds(num) / 1000.0


# --------------------------------------------------------------------------


def microseconds(num):
    """
    Expresses *num* microseconds in terms of fractions of a second.

    Args:
        num (float):
            The number of microseconds.

    Returns:
        float: *num* microseconds expressed in terms of seconds.
    """
    return milliseconds(num) / 1000.0


# --------------------------------------------------------------------------


def datetime(year, month, day, hour=0, minute=0, second=0, microsecond=0):
    """
    Returns the number of seconds since UNIX time (January 1, 1970, 00:00:00).

    Args:
        year (int):
            Year component of the date.

        month (int):
            Month component of the date.

        day (int):
            Day component of the date.

        hour (int, optional):
            Hour component of the date.

        minute (int, optional):
            Minute component of the date.

        second (int, optional):
            Second component of the date.

        microsecond (int, optional):
            Microsecond component of the date.
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
