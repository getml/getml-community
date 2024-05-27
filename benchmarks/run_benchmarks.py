"""
Runs the benchmark scripts
"""

from typing import Callable, Sequence

from benchmarks import (
    benchmark_air_pollution,
    benchmark_dodgers,
    benchmark_energy,
    benchmark_interstate94,
    benchmark_tetuan,
)
from benchmarks.drivers import (
    drive_featuretools,
    drive_getml,
    drive_kats,
    drive_tsfel,
    drive_tsflex,
    drive_tsfresh,
)

RUNS = 1

drivers_without_kats: Sequence[Callable] = [
    drive_getml,
    drive_tsflex,
    drive_featuretools,
    drive_tsfel,
    drive_tsfresh,
]


all_drivers: Sequence[Callable] = drivers_without_kats + [drive_kats]


air_pollution = benchmark_air_pollution(all_drivers, runs=RUNS)
print(air_pollution)
interstate94 = benchmark_interstate94(all_drivers, runs=RUNS)
print(interstate94)
dodgers = benchmark_dodgers(all_drivers, runs=RUNS)
print(dodgers)
energy = benchmark_energy(drivers_without_kats, runs=RUNS)
print(energy)
tetuan = benchmark_tetuan(all_drivers, runs=RUNS)
print(tetuan)

print()
print()

print(air_pollution)
print(interstate94)
print(dodgers)
print(energy)
print(tetuan)
