"""
Contains the main benchmark functions
"""

import logging
import os
from urllib import request
from typing import Callable, Sequence

import pandas as pd  # type: ignore

import getml

from .demo import Benchmark
from .utils import log_run, project

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def _load_csv(fname: str, url: str) -> pd.DataFrame:
    if not os.path.exists(fname):
        fname, _ = request.urlretrieve(url + fname, fname)
    return pd.read_csv(fname)


def _zero_padding(date_time: str) -> str:
    hour = date_time.split(" ")[1].split(":")[0]
    hour = "0" + hour if len(hour) == 1 else hour
    return (
        date_time.split(" ")[0]
        + " "
        + hour
        + ":"
        + date_time.split(" ")[1].split(":")[1]
    )


def benchmark_air_pollution(
    benchmark_drivers: Sequence[Callable], runs: int = 1
) -> Benchmark:
    ds_name = "air_pollution"
    time_stamp = "date"
    frequency = getml.data.time.hours(1)
    horizon = getml.data.time.hours(1)
    memory = getml.data.time.hours(12)

    observer = Benchmark(ds_name)

    for run in range(1, runs + 1):
        with project(f"__benchmark_{ds_name}"):
            air_pollution = getml.datasets.load_air_pollution()

            air_pollution = air_pollution[
                ~air_pollution["pm2.5"].is_null() & ~air_pollution["pm2.5"].is_inf()
            ]

            print(air_pollution)

            split = getml.data.split.time(
                population=air_pollution,
                time_stamp=time_stamp,
                test=getml.data.time.datetime(2014, 1, 1),
            )

            time_series = getml.data.TimeSeries(
                population=air_pollution,
                split=split,
                alias=air_pollution.name,
                time_stamps=time_stamp,
                horizon=horizon,
                memory=memory,
                lagged_targets=True,
            )

            for driver in benchmark_drivers:
                with log_run(driver, ds_name, run, runs):
                    try:
                        observer = driver(
                            observer=observer,
                            time_series=time_series,
                            min_chunksize=int(memory // frequency),
                            num_features=1000,
                            frequency=frequency,
                        )
                    except Exception as exp:
                        print(f"Failed {exp}")
    return observer


def benchmark_dodgers(
    benchmark_drivers: Sequence[Callable], runs: int = 1
) -> Benchmark:
    ds_name = "dodgers"
    source = "https://archive.ics.uci.edu/ml/machine-learning-databases/event-detection/Dodgers.data"
    time_stamp = "ds"
    frequency = getml.data.time.minutes(5)
    horizon = getml.data.time.minutes(5)
    memory = getml.data.time.hours(2)

    observer = Benchmark(ds_name)

    for run in range(1, runs + 1):
        with project(f"__benchmark_{ds_name}"):
            dodgers_pd = pd.read_csv(source, header=None, names=["ds", "y"])
            dodgers_pd.ds = pd.to_datetime(dodgers_pd.ds)
            dodgers_pd = dodgers_pd[dodgers_pd.y > 0]
            dodgers = getml.DataFrame.from_pandas(
                dodgers_pd,
                name="dodgers",
                roles={
                    getml.data.roles.target: ["y"],
                    getml.data.roles.time_stamp: ["ds"],
                },
            )

            split = getml.data.split.time(
                population=dodgers,
                time_stamp=time_stamp,
                test=getml.data.time.datetime(2005, 8, 20),
            )

            time_series = getml.data.TimeSeries(
                population=dodgers,
                split=split,
                alias=dodgers.name,
                time_stamps=time_stamp,
                horizon=horizon,
                memory=memory,
                lagged_targets=True,
            )

            for driver in benchmark_drivers:
                with log_run(driver, ds_name, run, runs):
                    try:
                        observer = driver(
                            observer=observer,
                            time_series=time_series,
                            min_chunksize=int(memory // frequency),
                            num_features=1000,
                            frequency=frequency,
                        )
                    except Exception as exp:
                        print(f"Failed {exp}")

    return observer


def benchmark_energy(benchmark_drivers: Sequence[Callable], runs: int = 1) -> Benchmark:
    ds_name = "energy"
    time_stamp = "date"
    frequency = getml.data.time.minutes(10)
    horizon = getml.data.time.minutes(10)
    memory = getml.data.time.hours(2)

    observer = Benchmark(ds_name)

    url = "https://raw.githubusercontent.com/LuisM78/Appliances-energy-prediction-data/master/"

    data_frame = pd.concat(
        [_load_csv("training.csv", url), _load_csv("testing.csv", url)]
    )
    data_frame.sort_values(by=["date"])

    for run in range(1, runs + 1):
        with project(f"__benchmark_{ds_name}"):

            energy: getml.DataFrame = getml.DataFrame.from_pandas(data_frame, "energy")
            energy.set_role("date", getml.data.roles.time_stamp)
            energy.set_role("Appliances", getml.data.roles.target)
            energy.set_role(energy.roles.unused_float, getml.data.roles.numerical)

            split = getml.data.split.time(
                population=energy,
                time_stamp=time_stamp,
                test=getml.data.time.datetime(2016, 4, 1),
            )

            time_series = getml.data.TimeSeries(
                population=energy,
                split=split,
                alias="energy",
                time_stamps=time_stamp,
                horizon=horizon,
                memory=memory,
                lagged_targets=True,
            )

            for driver in benchmark_drivers:
                with log_run(driver, ds_name, run, runs):
                    try:
                        observer = driver(
                            observer=observer,
                            time_series=time_series,
                            min_chunksize=int(memory // frequency),
                            num_features=2000,
                            frequency=frequency,
                        )
                    except Exception as exp:
                        print(f"Failed {exp}")

    return observer


def benchmark_interstate94(benchmark_drivers: Sequence[Callable], runs=1) -> Benchmark:
    ds_name = "interstate94"
    time_stamp = "ds"
    frequency = getml.data.time.hours(1)
    horizon = getml.data.time.hours(1)
    memory = getml.data.time.hours(24)

    observer = Benchmark(ds_name)

    for run in range(1, runs + 1):
        with project(f"__benchmark_{ds_name}"):
            traffic = getml.datasets.load_interstate94()
            traffic = traffic.drop(traffic.roles.categorical)

            split = getml.data.split.time(
                traffic, time_stamp, test=getml.data.time.datetime(2018, 3, 15)
            )

            time_series = getml.data.TimeSeries(
                population=traffic,
                split=split,
                alias=traffic.name,
                time_stamps=time_stamp,
                horizon=horizon,
                memory=memory,
                lagged_targets=True,
            )

            for driver in benchmark_drivers:

                with log_run(driver, ds_name, run, runs):
                    try:
                        observer = driver(
                            observer=observer,
                            time_series=time_series,
                            min_chunksize=int(memory // frequency),
                            num_features=1000,
                            frequency=frequency,
                        )
                    except Exception as exp:
                        print(f"Failed {exp}")

    return observer


def benchmark_tetuan(benchmark_drivers: Sequence[Callable], runs: int = 1) -> Benchmark:
    ds_name = "tetuan"
    time_stamp = "DateTime"
    frequency = getml.data.time.minutes(10)
    horizon = getml.data.time.minutes(10)
    memory = getml.data.time.hours(2)

    observer = Benchmark(ds_name)

    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/00616/"

    raw_data = _load_csv("Tetuan%20City%20power%20consumption.csv", url)
    raw_data["DateTime"] = [
        _zero_padding(date_time) for date_time in raw_data["DateTime"]
    ]

    for run in range(1, runs + 1):
        with project(f"__benchmark_{ds_name}"):
            tetuan: getml.DataFrame = getml.data.DataFrame.from_pandas(
                raw_data, "tetuan"
            )
            tetuan.set_role(
                "DateTime", getml.data.roles.time_stamp, time_formats=["%n/%e/%Y %H:%M"]
            )
            tetuan.set_role(["Zone 1 Power Consumption"], getml.data.roles.target)
            tetuan.set_role(tetuan.roles.unused_float, getml.data.roles.numerical)

            split = getml.data.split.time(
                population=tetuan,
                time_stamp=time_stamp,
                test=getml.data.time.datetime(2017, 11, 1),
            )

            time_series = getml.data.TimeSeries(
                population=tetuan,
                split=split,
                alias="tetuan",
                time_stamps=time_stamp,
                horizon=horizon,
                memory=memory,
                lagged_targets=True,
            )

            for driver in benchmark_drivers:
                with log_run(driver, ds_name, run, runs):
                    try:
                        observer = driver(
                            observer=observer,
                            time_series=time_series,
                            min_chunksize=int(memory // frequency),
                            num_features=2000,
                            frequency=frequency,
                        )
                    except Exception as exp:
                        print(f"Failed {exp}")

    return observer
