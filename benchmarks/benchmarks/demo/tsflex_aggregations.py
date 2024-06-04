"""
Custom implementations of FastProp aggregations
for tsflex.
"""

import functools
from typing import Any, Callable, List, Tuple

import numpy as np
import pandas as pd  # type: ignore
from scipy.stats import kurtosis, mode, skew  # type: ignore


def _avg(arr) -> float:
    if len(arr) == 0:
        return 0.0
    return np.mean(arr)


def _count_above_mean(arr) -> float:
    if len(arr) == 0:
        return 0.0

    # Needed to catch numerical instability
    # issues
    if np.unique(arr).shape[0] == 1:
        return 0.0

    mean = np.mean(arr)
    filtered = [0.0 for v in arr if v > mean]
    return float(len(filtered))


def _count_below_mean(arr) -> float:
    if len(arr) == 0:
        return 0.0

    # Needed to catch numerical instability
    # issues
    if np.unique(arr).shape[0] == 1:
        return 0.0

    mean = np.mean(arr)
    filtered = [0.0 for v in arr if v > mean]
    return float(len(filtered))


def _count_distinct(arr) -> float:
    return float(len(np.unique(arr)))


def _count_distinct_over_count(arr) -> float:
    if len(arr) == 0:
        return np.nan
    count_distinct = float(len(np.unique(arr)))
    return count_distinct / float(len(arr))


def _count_minus_count_distinct(arr) -> float:
    if len(arr) == 0:
        return 0.0
    count_distinct = float(len(np.unique(arr)))
    return float(len(arr)) - count_distinct


def _is_earlier(tup1, tup2):
    if tup2[0] < tup1[0]:
        return tup2
    return tup1


def _first(arr: pd.Series) -> float:
    if len(arr) == 0:
        return np.nan
    return arr[0]


def _kurtosis(arr) -> float:
    if len(arr) == 0:
        return np.nan

    # There appear to be numerical instability
    # issues with the scipy implementation.
    if np.unique(arr).shape[0] == 1:
        return 0.0

    return kurtosis(arr, fisher=False)


def _last(arr: pd.Series) -> float:
    if len(arr) == 0:
        return np.nan
    return arr[-1]


def _make_tuples(arr, descending=False) -> List[Tuple[float, float]]:
    # TSFlex returns numpy arrays, so we have
    # to assume a constant stride.
    if descending:
        return list(zip((float(len(arr) - i) for i in range(len(arr))), arr))
    return list(zip((float(i) for i in range(len(arr))), arr))


def _max(arr) -> float:
    if len(arr) == 0:
        return np.nan
    return np.max(arr)


def _median(arr) -> float:
    if len(arr) == 0:
        return np.nan
    return np.median(arr)


def _min(arr) -> float:
    if len(arr) == 0:
        return np.nan
    return np.min(arr)


def _mode(arr) -> float:
    if len(arr) == 0:
        return np.nan
    result = mode(arr)[0][0]
    try:
        return float(result)
    except ValueError:
        return np.nan


def _num_max(arr) -> float:
    if len(arr) == 0:
        return np.nan
    maximum = np.max(arr)
    filtered = [0.0 for v in arr if v == maximum]
    return float(len(filtered))


def _num_min(arr) -> float:
    if len(arr) == 0:
        return np.nan
    maximum = np.min(arr)
    filtered = [0.0 for v in arr if v == maximum]
    return float(len(filtered))


def _quantile(arr, quantile) -> float:
    if len(arr) == 0:
        return np.nan
    return float(np.quantile(arr, quantile, interpolation="linear"))


def _q1(arr) -> float:
    return _quantile(arr, 0.01)


def _q5(arr) -> float:
    return _quantile(arr, 0.05)


def _q10(arr) -> float:
    return _quantile(arr, 0.1)


def _q25(arr) -> float:
    return _quantile(arr, 0.25)


def _q75(arr) -> float:
    return _quantile(arr, 0.75)


def _q90(arr) -> float:
    return _quantile(arr, 0.9)


def _q95(arr) -> float:
    return _quantile(arr, 0.95)


def _q99(arr) -> float:
    return _quantile(arr, 0.99)


def _skew(arr) -> float:
    if not len(arr) == 0:
        return np.nan

    # There appear to be numerical instability
    # issues with the scipy implementation.
    if np.unique(arr).shape[0] == 1:
        return 0.0

    return skew(arr)


def _sum(arr) -> float:
    if len(arr) == 0:
        return 0.0
    return np.sum(arr)


def _stddev(arr) -> float:
    if not len(arr) == 0:
        return np.nan
    return np.std(arr)


def _time_since_first_maximum(arr) -> float:
    if len(arr) == 0:
        return np.nan

    def _is_better(tup1, tup2):
        if tup2[1] == tup1[1]:
            if tup2[0] > tup1[0]:
                return tup2
            return tup1
        if tup2[1] < tup1[1]:
            return tup2
        return tup1

    tuples = _make_tuples(arr)

    return functools.reduce(_is_better, tuples, tuples[0])[0]


def _time_since_first_minimum(arr) -> float:
    if len(arr) == 0:
        return np.nan

    def _is_better(tup1, tup2):
        if tup2[1] == tup1[1]:
            if tup2[0] < tup1[0]:
                return tup2
            return tup1
        if tup2[1] < tup1[1]:
            return tup2
        return tup1

    tuples = _make_tuples(arr)

    return functools.reduce(_is_better, tuples, tuples[0])[0]


def _time_since_last_maximum(arr) -> float:
    if len(arr) == 0:
        return np.nan

    def _is_better(tup1, tup2):
        if tup2[1] == tup1[1]:
            if tup2[0] > tup1[0]:
                return tup2
            return tup1
        if tup2[1] > tup1[1]:
            return tup2
        return tup1

    tuples = _make_tuples(arr)

    return functools.reduce(_is_better, tuples, tuples[0])[0]


def _time_since_last_minimum(arr) -> float:
    if len(arr) == 0:
        return np.nan

    def _is_better(tup1, tup2):
        if tup2[1] == tup1[1]:
            if tup2[0] > tup1[0]:
                return tup2
            return tup1
        if tup2[1] < tup1[1]:
            return tup2
        return tup1

    tuples = _make_tuples(arr)

    return functools.reduce(_is_better, tuples, tuples[0])[0]


def _trend(arr) -> float:
    if len(arr) == 0:
        return np.nan
    tuples = _make_tuples(arr, descending=True)
    mean_x = np.mean([v[0] for v in tuples])
    mean_y = np.mean([v[1] for v in tuples])
    sum_xx = np.sum([(v[0] - mean_x) * (v[0] - mean_x) for v in tuples])
    if sum_xx == 0.0:
        return mean_y
    sum_xy = np.sum([(v[0] - mean_x) * (v[1] - mean_y) for v in tuples])
    beta = sum_xy / sum_xx
    return mean_y - beta * mean_x


def _var(arr) -> float:
    if len(arr) == 0:
        return np.nan
    return np.var(arr)


def _variation_coefficient(arr) -> float:
    mean = np.mean(arr)
    if mean == 0.0:
        return np.nan
    return np.var(arr) / mean


def _ewma(arr, half_life: float) -> float:
    tuples = _make_tuples(arr, descending=True)
    weights = [np.exp(v[0] / half_life) for v in tuples]
    sum_weights = np.sum(weights)
    if sum_weights == 0.0:
        return np.nan
    sum_all = np.dot([v[1] for v in tuples], weights)
    return sum_all / sum_weights


def _make_ewma_aggs(freq: float) -> List[Callable[[Any], float]]:
    t1s = 1.0 / (np.log(0.5) * freq)
    t1m = t1s * 60.0
    t1h = t1m * 60.0
    t1d = t1h * 24.0
    t7d = t1d * 7.0
    t30d = t1d * 30.0
    t90d = t1d * 90.0
    t365d = t1d * 365.0

    def _ewma_t1s(arr) -> float:
        return _ewma(arr, t1s)

    def _ewma_t1m(arr) -> float:
        return _ewma(arr, t1m)

    def _ewma_t1h(arr) -> float:
        return _ewma(arr, t1h)

    def _ewma_t1d(arr) -> float:
        return _ewma(arr, t1d)

    def _ewma_t7d(arr) -> float:
        return _ewma(arr, t7d)

    def _ewma_t30d(arr) -> float:
        return _ewma(arr, t30d)

    def _ewma_t90d(arr) -> float:
        return _ewma(arr, t90d)

    def _ewma_t365d(arr) -> float:
        return _ewma(arr, t365d)

    return [
        _ewma_t1s,
        _ewma_t1m,
        _ewma_t1h,
        _ewma_t1d,
        _ewma_t7d,
        _ewma_t30d,
        _ewma_t90d,
        _ewma_t365d,
    ]


def make_fastprop_aggregations(frequency: float) -> List[Callable[[Any], float]]:
    return [
        _avg,
        _count_above_mean,
        _count_below_mean,
        _count_distinct,
        _count_distinct_over_count,
        _count_minus_count_distinct,
        _first,
        _kurtosis,
        _last,
        len,
        _max,
        _median,
        _min,
        _mode,
        _num_max,
        _num_min,
        _q1,
        _q5,
        _q10,
        _q25,
        _q75,
        _q90,
        _q95,
        _q99,
        _skew,
        _sum,
        _stddev,
        _time_since_first_maximum,
        _time_since_first_minimum,
        _time_since_last_maximum,
        _time_since_last_minimum,
        _trend,
        _var,
        _variation_coefficient,
    ] + _make_ewma_aggs(frequency)
