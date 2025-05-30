
<p align="center" style="text-align: center;">
    <img width="400" style="width: 50% !important; max-width: 400px;" src="assets/getml_logo_dark.png#gh-dark-mode-only" />
    <img width="400" style="width: 50% !important; max-width: 400px;" src="assets/getml_logo.png#gh-light-mode-only" />
</p>

<p align="center" style="text-align: center;">
        <a href="https://getml.com/latest/contact" target="_blank">
        <img src="https://img.shields.io/badge/schedule-a_meeting-blueviolet.svg" /></a>
        <a href="mailto:hello@getml.com" target="_blank">
        <img src="https://img.shields.io/badge/contact-us_by_mail-orange.svg" /></a>
        <a href="LICENSE.txt" target="_blank">
        <img src="https://img.shields.io/badge/LICENSE-ELv2-green" /></a>
</p>

# getML - Automated Feature Engineering for Relational Data and Time Series

## Introduction

getML is a tool for automating feature engineering on relational data and time series. It includes a specifically customized database Engine for this very purpose.

This results in a speedup between _60_ to _1000_ times (see [Benchmarks](#benchmarks)) over other open-source tools like [featuretools](https://www.featuretools.com) and [tsfresh](https://tsfresh.com) for automated feature engineering. Also check out our [demonstrational notebooks](https://getml.com/latest/examples) to see more comparisons.

## Table of Contents

* [Introduction](#introduction)
* [Table of Contents](#table-of-contents)
* [Quick Start](#quick-start)
* [Key benefits of using getML](#key-benefits-of-using-getml)
  * [Features generate by getML](#features-generate-by-getml)
  * [Documentation](#documentation)
* [Benchmarks](#benchmarks)
* [Demo notebooks](#demo-notebooks)
* [Example](#example)
* [Release Notes](#release-notes)
* [Development](#development)

## Quick Start

As getML is available on [PyPI](https://pypi.org/project/getml), you can install it simply via

```bash
pip install getml
```

> [!NOTE]
> To get started on macOS and Windows, you first need to [start the getML docker service](https://getml.com/latest/install/packages/docker/):
> ```sh
> # run the lastest version
> curl -L https://raw.githubusercontent.com/getml/getml-community/refs/heads/main/runtime/docker-compose.yml | docker compose -f - up
> 
> # run a specific version, e.g. 1.5.1
> # curl https://raw.githubusercontent.com/getml/getml-community/1.5.1/runtime/docker-compose.yml | docker compose -f - up
> ```

Check out [the Example](#example) and the [demonstrational notebooks](https://getml.com/latest/examples) to get started with getML. A [detailed walkthrough guide](https://getml.com/latest/user_guide/walkthrough) and [the documentation](https://getml.com/latest) will also help you on your way with getML.

To learn, how to build and contribute to getML, check out [BUILD.md](BUILD.md) for instructions on how to build getML from source.

## Key benefits of using getML

One big key feature over other tools like [featuretools](https://www.featuretools.com), [tsfresh](https://tsfresh.com) and [prophet](https://facebook.github.io/prophet) is the runtime performance. Our own implementation of propositionalization, FastProp (short for fast propositionalization), reaches improvements of about _60_ to _1000_ times faster run times (see specifically [FastProp Benchmarks](https://getml.com/latest/examples/enterprise-notebooks/fastprop_benchmark) within our notebooks). This leads to faster iterations for data scientists, giving them more time to tweak variables to achieve even better results.

FastProp is not only faster, but can also provide an increased accuracy.

For even better accuracy, getML provides advanced algorithms in its [professional and Enterprise feature-sets](https://getml.com/latest/enterprise/benefits), namely [Multirel](https://getml.com/latest/user_guide/concepts/feature_engineering/#feature-engineering-algorithms-multirel), [Relboost](https://getml.com/latest/user_guide/concepts/feature_engineering/#feature-engineering-algorithms-relboost), [Fastboost](https://getml.com/latest/user_guide/concepts/feature_engineering/#feature-engineering-algorithms-fastboost) and [RelMT](https://getml.com/latest/user_guide/concepts/feature_engineering/#feature-engineering-algorithms-relmt).

The standard version includes [preprocessors](https://getml.com/latest/user_guide/concepts/preprocessing) (like CategoryTrimmer, EmailDomain, Imputation, Mapping, Seasonal, Substring, TextFieldSplitter), [predictors](https://getml.com/latest/user_guide/concepts/predicting) (like LinearRegression, LogisticRegression, XGBoostClassifier, XGBoostRegressor) and [hyperparameter optimizer](https://getml.com/latest/user_guide/concepts/hyperopt) (like RandomSearch, LatinHypercubeSearch, GaussianHyperparameterSearch).

It also gives access to [the getML Monitor](https://getml.com/latest/user_guide/concepts/getml_suite/#monitor-concepts), which provides valuable information about projects, pipelines, features, important columns, accuracies, performances, and more. Those information give insights and help understand and improve the results.

getML can [import data from various sources](https://getml.com/latest/user_guide/concepts/importing_data) like CSV, Pandas, JSON, SQLite, MySQL, MariaDB, PostgreSQL, Greenplum, ODBC.

While the standard version is open source, can be run on your local machine, and gets basic support via EMail and via this repository, it must not be used for productive purposes. The [professional and Enterprise versions](https://getml.com/latest/enterprise/benefits) in contrast allows productive uses, gets also support via phone and chat, offers training sessions, as well as on-premise and cloud hosting, and export and deployment features. Get in [contact via email](mailto:hello@getml.com) or directly [schedule a meeting](https://getml.com/latest/contact).

### Features generate by getML

getML generates features for relational data and time series. These include, but are not limited to:

* **Various aggregations**, i.e. average, sum, minimum, maximum, quantiles, exponentially weighted moving average, trend, exponentially weighted trends, ...
* **Aggregations within a certain time frame**, i.e. maximum in the last 30 days, minimum in the last 7 days
* **Seasonal factors from time stamps**, such as month, day of the week, hour, ...
* **Seasonal aggregations**, i.e. maximum for the same weekday as the prediction point, minimum for the same hour as the prediction point, ...

In other words, it generates the kind of features you would normally build manually. But it automatically generates thousands of features and then automatically picks the best, saving you a lot of manual work.

### Documentation

Check out the full documentation on <https://getml.com/latest>

## Benchmarks

We evaluated the performance of [getML's FastProp algorithm](https://getml.com/latest/user_guide/concepts/feature_engineering/#feature-engineering-algorithms-fastprop) against five other open-source tools for automated feature engineering on relational data and time series: [_tsflex_](https://github.com/predict-idlab/tsflex), [_featuretools_](https://www.featuretools.com), [_tsfel_](https://github.com/fraunhoferportugal/tsfel), [_tsfresh_](https://github.com/blue-yonder/tsfresh) and [_kats_](https://github.com/facebookresearch/Kats). The datasets used include:

1. Air Pollution
    * Hourly data on air pollution and weather in Beijing, China.
2. Interstate94
    * Hourly data on traffic volume on the Interstate 94 from Minneapolis to St. Paul.
3. Dodgers
    * Five-minute measurements of traffic near Los Angeles, affected by games hosted by the LA Dodgers.
4. Energy
    * Ten-minute measurements of the electricity consumption of a single household.
5. Tetouan
    * Ten-minute electricity consumption of three different zones in Tetouan City, north Morocco

The plots shown below contain the _runtime per feature_ calculated relative to the runtime per feature of the fastest approach. The fastest approach turns out to be the getML's FastProp, so it gets a value 1.

We observe, that for all datasets, the features produced by the different tools are quite similar, but **getML is 60-1000 times faster** than other open-source tools.

<p align="center">
    <img style="width: 80%" src="assets/benchmarks_plot_linear.png" />
</p>

In fact, the speed-up is so big that we need a logarithmic scale to even see the bar for getML.

<p align="center">
    <img style="width: 80%" src="assets/benchmarks_plot_log.png" />
</p>

To reproduce those results, refer to the [benchmarks folder](benchmarks) in this repository.

## Demo notebooks

To experience getML in action, the following example notebooks are provided in the [demo-notebooks](demo-notebooks) directory:

| Notebook                                                      | Prediction Type  | Population Size | Data Type   | Target  | Domain           | Difficulty | Comments                                        |
| ------------------------------------------------------------- | ---------------- | --------------- | ----------- |-------- | ---------------- | ---------- | ----------------------------------------------- |
| [adventure_works.ipynb](demo-notebooks/adventure_works.ipynb) | Classification   | 19,704          | Relational  | Churn   | Customer loyalty | Hard       | Good reference for a complex data model         |
| [formula1.ipynb](demo-notebooks/formula1.ipynb)               | Classification   | 31,578          | Relational  | Win     | Sports           | Medium     |                                                 |
| [interstate94.ipynb](demo-notebooks/interstate94.ipynb)       | Regression       | 24,096          | Time Series | Traffic | Transportation   | Easy       | Good notebook to get started on time series     |
| [loans.ipynb](demo-notebooks/loans.ipynb)                     | Classification   | 682             | Relational  | Default | Finance          | Easy       | Good notebook to get started on relational data |
| [robot.ipynb](demo-notebooks/robot.ipynb)                     | Regression       | 15,001          | Time Series | Force   | Robotics         | Medium     |                                                 |
| [seznam.ipynb](demo-notebooks/seznam.ipynb)                   | Regression       | 1,462,078       | Relational  | Volume  | E-commerce       | Medium     |                                                 |

For an extensive list of demonstrational and benchmarking notebooks, have a look at [our docs examples section](https://getml.com/latest/examples) or [the notebook source repository](https://github.com/getml/getml-demo).

## Example

Here is how you can build a complete Data Science pipeline for a time series problem with seasonalities with just a few lines of code:

```python
import getml

getml.engine.launch()
getml.set_project("interstate94")

# Load the data.
traffic = getml.datasets.load_interstate94(roles=False, units=False)

# Set the roles, so getML knows what you want to predict
# and which columns you want to use.
traffic.set_role("ds", getml.data.roles.time_stamp)
traffic.set_role("holiday", getml.data.roles.categorical)
traffic.set_role("traffic_volume", getml.data.roles.target)

# Generate a train/test split using 2018/03/15 as the cutoff date.
split = getml.data.split.time(traffic, "ds", test=getml.data.time.datetime(2018, 3, 15))

# Set up the data:
# - We want to predict the traffic volume for the next hour.
# - We want to use data from the seven days before the reference date.
# - We want to use lagged targets (autocorrelated features are allowed).
time_series = getml.data.TimeSeries(
    population=traffic,
    split=split,
    time_stamps="ds",
    horizon=getml.data.time.hours(1),
    memory=getml.data.time.days(7),
    lagged_targets=True,
)

# The Seasonal preprocessor extracts seasonal
# components from the time stamps.
seasonal = getml.preprocessors.Seasonal()

# FastProp extracts features from the time series.
fast_prop = getml.feature_learning.FastProp(
    loss_function=getml.feature_learning.loss_functions.SquareLoss,
    num_threads=1,    
    num_features=20,
)

# Use XGBoost for the predictions (it comes out-of-the-box).
predictor = getml.predictors.XGBoostRegressor()

# Combine them all in a pipeline.
pipe = getml.pipeline.Pipeline(
    tags=["memory: 7d", "horizon: 1h", "fast_prop"],
    data_model=time_series.data_model,
    preprocessors=[seasonal],
    feature_learners=[fast_prop],
    predictors=[predictor],
)

# Fit on the train set and evaluate on the testing set.
pipe.fit(time_series.train)
pipe.score(time_series.test)
predictions = pipe.predict(time_series.test)
```

To see the full example, check out the Interstate94 notebook ([interstate94.ipynb](https://getml.com/latest/examples/community-notebooks/interstate94)).

## Release Notes
See [CHANGELOG.md](CHANGELOG.md) for release notes.

## Development
### Python venv
To create the virtual environment for the [python api](src/python-api) just run
`uv sync` from anywhere in the project. If you have 
[mise hooked into your shell](https://mise.jdx.dev/getting-started.html#activate-mise) the 
environment will be activated automatically whenever you your prompt is reloaded
inside the project. If you don't have mise hooked into your shell, you can drop
into a shell populated with the environment by running `mise en`.

> [!NOTE]
> mise doesn't pick up the environment automatically after creating the environment with `uv sync`.
> To create and activate the environment in one go run:
> ```sh
> uv sync && mise en
> ```

### Building getml
Refer to [BUILD.md](BUILD.md) for build instructions.
