
<p align="center" style="text-align: center;">
    <img width="400" style="width: 50% !important; max-width: 400px;" src="assets/getml_logo_dark.png#gh-dark-mode-only" />
    <img width="400" style="width: 50% !important; max-width: 400px;" src="assets/getml_logo.png#gh-light-mode-only" />
</p>

<p align="center" style="text-align: center;">
        <a href="https://getml.com/contact" target="_blank">
        <img src="https://img.shields.io/badge/schedule-a_meeting-blueviolet.svg" /></a>
        <a href="mailto:hello@getml.com" target="_blank">
        <img src="https://img.shields.io/badge/contact-us_by_mail-orange.svg" /></a>
        <a href="LICENSE.txt" target="_blank">
        <img src="https://img.shields.io/badge/LICENSE-ELv2-green" /></a>
</p>

# getML - Automated Feature Engineering for Relational Data and Time Series

## Introduction

getML is a tool for automating feature engineering on relational data and time series. It includes a specifically customized database engine for this very purpose.

This results in a speedup between _60_ to _1000_ times (see [Benchmarks](#benchmarks)) over other open-source tools like [featuretools](https://www.featuretools.com/) and [tsfresh](https://tsfresh.com/) for automated feature engineering. Also check out our [demonstrational notebooks](https://github.com/getml/getml-demo) to see more comparisons.

## Table of Contents

* [Introduction](#introduction)
* [Table of Contents](#table-of-contents)
* [Quick Start](#quick-start)
* [Key benefits for using getML](#key-benefits-for-using-getml)
  * [Features generate by getML](#features-generate-by-getml)
  * [Documentation](#documentation)
* [Benchmarks](#benchmarks)
* [Demo notebooks](#demo-notebooks)
* [Example](#example)

## Quick Start

As getML is available on [PyPI](https://pypi.org/project/getml/), you can install it simply via

```bash
$ pip install getml
```

Check out [the Example](#example) and the [demonstrational notebooks](https://github.com/getml/getml-demo) to get started with getML. A [detailed getting started guide](https://docs.getml.com/latest/home/getting_started/getting_started.html) and [the documentation](https://docs.getml.com/latest/) will also help you on your way with getML.

To learn, how to build and contribute to getML, check out [CONTRIBUTING.md](CONTRIBUTING.md)

## Key benefits for using getML

One big key feature over other tools like [featuretools](https://www.featuretools.com/), [tsfresh](https://tsfresh.com/) and [prophet](https://facebook.github.io/prophet/) is the runtime performance. The own implementations of FastProp (short for fast propositionalization) of a propositionalization algorithm reaches improvements of about _60_ to _160_ times faster run times (see specifically [FastProp Benchmarks](https://github.com/getml/getml-demo/README.md#fastprop-benchmarks) within our notebooks). This leads to faster iterations for data scientists, giving them more time to tweak variables to achieve even better results.

FastProp is not only faster, but can also provide an increased accuracy.

For even better accuracy, getML provides advanced algorithms in its [professional and enterprise feature-sets](https://www.getml.com/pricing), namely [Multirel](https://docs.getml.com/latest/user_guide/feature_engineering/feature_engineering.html#multirel), [Relboost](https://docs.getml.com/latest/user_guide/feature_engineering/feature_engineering.html#relboost) and [RelMT](https://docs.getml.com/latest/user_guide/feature_engineering/feature_engineering.html#relmt). 

The standard version includes [preprocessors](https://docs.getml.com/latest/user_guide/preprocessing/preprocessing.html) (like CategoryTrimmer, EmailDomain, Imputation, Mapping, Seasonal, Substring, TextFieldSplitter), [predictors](https://docs.getml.com/latest/user_guide/predicting/predicting.html#using-getml) (like LinearRegression, LogisticRegression, XGBoostClassifier, XGBoostRegressor) and [hyperparameter optimizer](https://docs.getml.com/latest/user_guide/hyperopt/hyperopt.html#hyperparameter-optimization) (like RandomSearch, LatinHypercubeSearch, GaussianHyperparameterSearch).

It also gives access to [the getML Monitor](https://docs.getml.com/latest/user_guide/getml_suite/monitor.html), which provides valuable information about projects, pipelines, features, important columns, accuracies, performances, and more. Those information give insights and help understand and improve the results.

getML can [import data from various sources](https://docs.getml.com/latest/user_guide/importing_data/importing_data.html) like CSV, Pandas, JSON, SQLite, MySQL, MariaDB, PostgreSQL, Greenplum, ODBC. 

While the standard version is open source, can be run on your local machine, and gets basic support via EMail and via this repository, it can't be used for productive purposes. The [professional and enterprise versions](https://www.getml.com/pricing) in contrast allows productive uses, gets also support via phone and chat, offers training sessions, as well as on-premise and cloud hosting, and export and deployment features. Get in [contact via email](mailto:hello@getml.com) or directly [schedule a meeting](https://getml.com/contact).

### Features generate by getML

getML generates features for relational data and time series. These include, but are not limited to:

- **Various aggregations**, i.e. average, sum, minimum, maximum, quantiles, exponentially weighted moving average, trend, exponentially weighted trends, ...
- **Aggregations within a certain time frame**, i.e. maximum in the last 30 days, minimum in the last 7 days
- **Seasonal factors from time stamps**, such as month, day of the week, hour, ...
- **Seasonal aggregations**, i.e. maximum for the same weekday as the prediction point, minimum for the same hour as the prediction point, ...

In other words, it generates the kind of features you would normally build manually. But it automatically generates thousands of features and then automatically picks the best, saving you a lot of manual work.

### Documentation

Check out the full documentation on https://docs.getml.com/latest/.

## Benchmarks

We evaluated the performance of [getML's FastProp algorithm](https://docs.getml.com/latest/user_guide/feature_engineering/feature_engineering.html#fastprop) against five other open-source tools for automated feature engineering on relational data and time series: [_tsflex_](https://github.com/predict-idlab/tsflex), [_featuretools_](https://www.featuretools.com/), [_tsfel_](https://github.com/fraunhoferportugal/tsfel), [_tsfresh_](https://github.com/blue-yonder/tsfresh) and [_kats_](https://github.com/facebookresearch/Kats). The datasets used include:

1. Air Pollution 
    * Hourly data on air pollution and weather in Beijing, China.
2. Interstate94 
    * Hourly data on traffic volume on the Interstate 94 from Minneapolis to St. Paul.
3. Dodgers
    * Five-minute measurements of traffic near Los Angeles, affected by games hosted by the LA Dodgers.
4. Energy
    * Ten-minute measurements of the electricity consumption of a single household.
5. Tetouan
    * Ten-minute electricity consumption of three different zones in Tetouan City, Mexico

The plots shown below contain the *runtime per feature* calculated relative to the runtime per feature of the fastest approach. The fastest approach turns out to be the getML's FastProp, so it gets a value 1.

We observe, that for all datasets, the features produced by the different tools are quite similar, but __getML is 60-1000 times faster__ than other open-source tools. 

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
| ------------------------------------------------------------- | ---------------- | --------------- | ----------- |-------- | ---------------- | ---------- | --------------------------- |
| [interstate94.ipynb](demo-notebooks/interstate94.ipynb)       | Regression       | 24,096          | Time Series | Traffic | Transportation   | Easy       | Good notebook to get started on time series     |
| [loans.ipynb](demo-notebooks/loans.ipynb)                     | Classification   | 682             | Relational  | Default | Finance          | Easy       | Good notebook to get started on relational data |
| [robot.ipynb](demo-notebooks/robot.ipynb)                     | Regression       | 15,001          | Time Series | Force   | Robotics         | Medium     |                                                 |

For an extensive list of demonstrational and benchmarking notebooks, have a look at the [their own repository](https://github.com/getml/getml-demo).

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

To see the full example, check out the Interstate94 notebook ([interstate94.ipynb](demo-notebooks/interstate94.ipynb)).