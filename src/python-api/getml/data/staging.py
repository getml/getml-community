# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#


"""
Collection of functions for simulating the effects of staging,
not meant to be used by the end user.
"""

from typing import List, Optional, Tuple

from .placeholder import Join, Placeholder
from .relationship import _all_relationships, many_to_one, one_to_one

# ------------------------------------------------------------------

Pair = Tuple[List[str], List[Join]]

# ------------------------------------------------------------------


def _compare_join(join1: Join, join2: Join) -> bool:
    return (
        (join1.right.name == join2.right.name)
        and (join1.on == join2.on)
        and (join1.time_stamps == join2.time_stamps)
        and (join1.upper_time_stamp == join2.upper_time_stamp)
        and (join1.relationship == join2.relationship)
        and (join1.memory == join2.memory)
        and (join1.horizon == join2.horizon)
        and (join1.lagged_targets == join2.lagged_targets)
    )


# ------------------------------------------------------------------


def _compare_joins(pair1: Pair, pair2: Pair) -> bool:
    return all(_compare_join(j1, j2) for (j1, j2) in zip(pair1[1], pair2[1]))


# ------------------------------------------------------------------


def _is_same(pair1: Pair, pair2: Pair) -> bool:
    if pair1[0] != pair2[0]:
        return False

    if not _compare_joins(pair1, pair2):
        return False

    return True


# ------------------------------------------------------------------


def _is_to_one(relationship: Optional[str]) -> bool:
    if not relationship:
        raise ValueError("'relationship' is not set!")
    if relationship not in _all_relationships:
        raise ValueError(
            "'relationship' must be from getml.data.relationship, "
            + "meaning it must be one of the following: "
            + str(_all_relationships)
            + "."
        )
    return relationship in [many_to_one, one_to_one]


# ------------------------------------------------------------------


def _make_names(placeholder: Placeholder) -> List[str]:
    return [placeholder.name] + [
        name
        for j in placeholder.joins
        if _is_to_one(j.relationship)
        for name in _make_names(j.right)
    ]


# ------------------------------------------------------------------


def _make_joins(placeholder: Placeholder) -> List[Join]:
    return [join for join in placeholder.joins if _is_to_one(join.relationship)] + [
        subjoin
        for join in placeholder.joins
        if _is_to_one(join.relationship)
        for subjoin in _make_joins(join.right)
    ]


# ------------------------------------------------------------------


def _make_pair(placeholder: Placeholder) -> Pair:
    names = _make_names(placeholder)
    joins = _make_joins(placeholder)
    assert len(names) == len(joins) + 1, "Lengths don't match"
    return (names, joins)


# ------------------------------------------------------------------


def _make_list_of_pairs(placeholder: Placeholder, include_head=True) -> List[Pair]:
    head = [_make_pair(placeholder)] if include_head else []
    return head + [
        pair
        for j in placeholder.joins
        for pair in _make_list_of_pairs(j.right, not _is_to_one(j.relationship))
    ]


# ------------------------------------------------------------------


def _make_staging_overview(placeholder: Placeholder) -> List[List[str]]:
    """
    All joins that involve many-to-one or one-to-one joins are handled
    during the staging phase. Therefore, we need to:
    1) Identify any such joins (_make_list_of_pairs)
    2) Sort the peripheral tables by the name of the first table.
    3) Remove any duplicates (_remove_duplicates).
    4) Return a list that maps the names of the original tables
       to the name of the generated staging tables.
    """
    list_of_pairs = _make_list_of_pairs(placeholder)
    peripheral = list_of_pairs[1:]
    peripheral.sort(key=lambda pairs: pairs[0][0])
    peripheral = _remove_duplicates(peripheral)
    list_of_names = [list_of_pairs[0][0]] + [p[0] for p in peripheral]
    return [
        [", ".join(lis), lis[0].upper() + "__STAGING_TABLE_" + str(i + 1)]
        for (i, lis) in enumerate(list_of_names)
    ]


# ------------------------------------------------------------------


def _remove_duplicates(sorted_list_of_pairs: List[Pair]) -> List[Pair]:
    if not sorted_list_of_pairs:
        return []

    if len(sorted_list_of_pairs) == 1:
        return sorted_list_of_pairs

    head = sorted_list_of_pairs[0]

    tail = sorted_list_of_pairs[1:]

    if _is_same(head, tail[0]):
        return _remove_duplicates(tail)

    return [head] + _remove_duplicates(tail)


# ------------------------------------------------------------------
