"""
Runs the benchmark scripts
"""

from .benchmarks import benchmark_interstate94
from .drivers import (
    drive_getml,
    drive_featuretools,
    drive_tsfel,
    drive_tsflex,
    drive_tsfresh,
)

all_drivers = [drive_getml, drive_tsflex]

benchmark_interstate94(all_drivers)
