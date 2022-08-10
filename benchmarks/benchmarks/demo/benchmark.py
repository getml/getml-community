from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import timedelta
from time import perf_counter
from typing import Any, Dict, Generator, List, Optional

import numpy as np

from getml import Pipeline
from getml.pipeline.metrics import _all_metrics


@dataclass
class BenchmarkData:
    ds_name: str
    lib_name: str
    num_features: List[int] = field(default_factory=list)
    runtimes: List[Optional[timedelta]] = field(default_factory=list)
    scores: List[Dict[str, float]] = field(default_factory=list)


@dataclass
class Row:
    ds_name: str
    lib_name: str
    num_features: int
    runtime: Any
    score: Dict[str, Any]

    def to_list(
        self, best_runtime: timedelta, best_runtime_per_feature: timedelta
    ) -> List[Any]:

        return [
            self.ds_name,
            self.lib_name,
            self.num_features,
            f"{self.runtime}",
            f"{self.runtime/self.num_features}",
            f"{self.runtime/best_runtime}",
            f"{(self.runtime/self.num_features)/best_runtime_per_feature}",
            {
                name: f"{val:.3f}"
                for name, val in self.score.items()
                if name in _all_metrics
            },
        ]


def _make_row(data: BenchmarkData) -> Row:
    runtimes = [runtime for runtime in data.runtimes if runtime is not None]
    runtime = sum(runtimes, timedelta(0)) / float(len(runtimes))
    return Row(
        ds_name=data.ds_name,
        lib_name=data.lib_name,
        num_features=data.num_features[0],
        runtime=runtime,
        score=data.scores[0],
    )


class Benchmark:
    def __init__(self, ds_name: str) -> None:
        self.ds_name = ds_name
        self._data: Dict[str, Any] = {}

    @contextmanager
    def __call__(self, lib_name: str) -> Generator[None, None, None]:
        with self.benchmark(lib_name):
            yield

    def __getattr__(self, lib_name) -> BenchmarkData:
        try:
            return self._data[lib_name]
        except KeyError:
            return super().__getattribute__(lib_name)

    def __repr__(self) -> str:

        template = "{:15}  {:12}  {:>8}  {:14}  {:14}  {:20}  {:25} {}"

        rows = [_make_row(benchmark) for benchmark in self._data.values()]

        best_runtime = min((row.runtime for row in rows))
        best_runtime_per_feature = min((row.runtime / row.num_features for row in rows))

        headers = [
            "ds_name",
            "lib_name",
            "num_feat",
            "runtime",
            "runtime/feat",
            "runtime (normalized)",
            "runtime/feat (normalized)",
            "scores",
        ]

        rows_as_list = [
            row.to_list(best_runtime, best_runtime_per_feature) for row in rows
        ]

        head = template.format(*headers)
        body = "\n".join([template.format(*row) for row in rows_as_list])

        return f"{head}\n{body}"

    @contextmanager
    def _benchmark_runtime(self, lib_name: str) -> Generator[Benchmark, None, None]:
        begin = perf_counter()
        yield self
        end = perf_counter()
        self._data[lib_name].runtimes.append(timedelta(seconds=end - begin))
        print(f"Time taken: {self._data[lib_name].runtimes[-1]}")

    @contextmanager
    def benchmark(self, lib_name: str) -> Generator[Benchmark, None, None]:
        if lib_name not in self._data:
            self._data[lib_name] = BenchmarkData(
                ds_name=self.ds_name, lib_name=lib_name
            )
        with self._benchmark_runtime(lib_name) as timer:
            yield timer

    @property
    def runtimes(self) -> Dict[str, List]:
        return {lib_name: data.runtimes for lib_name, data in self._data.items()}

    @property
    def scores(self) -> Dict[str, List]:
        return {lib_name: data.scores for lib_name, data in self._data.items()}

    def write_scores(self, lib_name: str, pipe: Pipeline, num_features: int) -> None:
        self._data[lib_name].num_features.append(num_features)
        self._data[lib_name].scores.append(dict(pipe.scores[-1]))
        print(f"Performance achieved: {self._data[lib_name].scores[-1]}")
