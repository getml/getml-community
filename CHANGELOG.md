# Release Notes

## For getML Enterprise & Community editions 

### 1.5.0   <small>Sep 24, 2024</small>
#### Features
- Overhaul and better integration of API documentation and web page:
    - Switch from [sphinx](https://www.sphinx-doc.org/en/master/) to [mkdocs](https://www.mkdocs.org/)
    - Restructuring of [User Guide](https://getml.com/latest/user_guide/), multiple amendments to documentation 
- Introduce strict typing regiment for [feature learning aggregations](https://getml.com/latest/reference/feature_learning/aggregations/) and [loss functions](https://getml.com/latest/reference/feature_learning/loss_functions)
- Clean up and maintenance of [example notebooks](https://getml.com/latest/examples/), make them executable in [Colab](https://colab.google/)
- More informative progress bar and status updates using [rich](https://github.com/Textualize/rich?tab=readme-ov-file)
- Completely reworked IO
    - Leveraging [PyArrow](https://arrow.apache.org/docs/python/index.html) to improve reliability, speed and maintainability
- Introduce [reflect-cpp](https://github.com/getml/reflect-cpp) for parsing and de/serialization
- Introduce overhauled getML Docker runtime [available from Docker Hub](https://hub.docker.com/r/getml/getml), allowing for easy setup
  - See [docker-related section of the new getML documentation](https://getml.com/latest/install/packages/docker/) for details
#### Developer-focused
- Complete rework of the build pipeline (docker and linux native)
    - Introduce [CCache](https://ccache.dev/), [conan](https://conan.io/), [vcpkg](https://vcpkg.io/en/)
    - User multi-stage docker builds leveraging buildx and buildkit
    - Centralized `VERSION`
- [Ruff](https://docs.astral.sh/ruff/) for linting and formatting
- [Hatch](https://hatch.pypa.io/latest/) for python package management
#### Bug fixes
- Generalization of [`Placeholder.join`](https://getml.com/latest/reference/data/placeholder/#getml.data.Placeholder.join)'s `on` argument 
- Improved timestamp handling
- Slicing improvements
    - Slicing of `DataFrames` returned wrong results: Remove short circuit for slices with upper bound
    - Introduce set semantics for slicing of `DataFrame` (return empty collections instead of erroring)
- Fix displaying of parameter lists with values that exceed the presentable width
- Fix displaying of [`DataFrames`](https://getml.com/latest/reference/data/data_frame/) with one row or less
- Fix progress bar output on Google Colab

### 1.4.0	<small>Oct 17, 2023</small>
- Accelerated feature learning through [Fastboost](https://getml.com/latest/reference/feature_learning/fastboost)
- Improved modelling on huge datasets through [ScaleGBMClassifier](https://getml.com/latest/reference/predictors/scale_gbm_classifier) and [ScaleGBMRegressor](https://getml.com/latest/reference/predictors/scale_gbm_regressor)
- Advanced trend aggregations using [EWMATrend aggregations](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.EWMA_1S)
- Faster JSON parsing using YYJSON

### 1.3.2	<small>Jan 26, 2023</small>
- Minor bugfixes

### 1.3.1	<small>Dec 20, 2022</small>
- Implement `tqdm` for progress bars
- Minor bugfixes

### 1.3.0	<small>Aug 28, 2022</small>
- Use websockets instead of polling
- Size [threshold](https://getml.com/latest/reference/pipeline.Features.to_sql) for better visualization of feature code
- Faster reading of memory-mapped data, relevant for all feature learners and predictors
- Introduce [CategoryTrimmer](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.CategoryTrimmer) as preprocessor

### 1.2.0	<small>May 20, 2022</small>
- Support for [SQL transpilation](https://getml.com/latest/reference/pipeline/dialect/): TSQL, Postgres, MySQL, BigQuery, Spark
- Support for memory mapping

### 1.1.0	<small>Nov 21, 2021</small>
- Enhance data processing by introducing Spark (e.g. [spark_sql](https://getml.com/latest/reference/pipeline/dialect/#getml.pipeline.dialect.spark_sql)) and Arrow (e.g. [from_arrow()](https://getml.com/latest/reference/data/data_frame/#getml.data.DataFrame.from_arrow))
- Integrate Vcpkg for dependency management
- Improve code transpilation for seasonal variables
- Better control of predictor training and hyperparamter optimization through introduction of early stopping (e.g. in [ScaleGBMClassifier](https://getml.com/latest/reference/predictors/scale_gbm_classifier/))
- Introduce [TREND](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.TREND) aggregation
- Better progress logging

### 1.0.0	<small>Sep 23, 2021</small>
- Introduction of [Containers](https://getml.com/latest/reference/data/container/) for data storage
- Complete overhaul of the API including [Views](https://getml.com/latest/reference/data/view/), [StarSchema](https://getml.com/latest/reference/data/star_schema/), [TimeSeries](https://getml.com/latest/reference/data/time_series/)
- Add [subroles](https://getml.com/latest/reference/data/subroles/) for fine grained data control
- Improved model evaluation through [Plots](https://getml.com/latest/reference/pipeline/plots/) and [Scores](https://getml.com/latest/reference/pipeline/scores_container/) container
- Introduce [slicing](https://getml.com/latest/reference/data/view/#getml.data.View.where) of Views
- Add [datetime()](https://getml.com/latest/reference/data/time/#getml.data.time.datetime) utility

### 0.16.0 <small>May 25, 2021</small>
- Add the [Mapping](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.Mapping) and [TextFieldSplitter](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.TextFieldSplitter) preprocessors

### 0.15.0 <small>Feb 23, 2021</small>
- Add the [Fastprop](https://getml.com/latest/reference/feature_learning/fastprop/) feature learner
- Overhaul the way RelMT and Relboost generate features, making them more efficient

### 0.14.0 <small>Jan 18, 2021</small>
- Significant improvement of  project management:
    -  [project.restart()](https://getml.com/latest/reference/project/#getml.project.attrs.restart), [project.suspend()](https://getml.com/latest/reference/project/#getml.project.attrs.suspend), and [project.switch()](https://getml.com/latest/reference/project/#getml.project.attrs.switch)
    - multiple project support
- Add custom `__getattr__` and `__dir__` methods to DataFrame, enabling column retrieval through autocomplete

### 0.13.0 <small>Nov 13, 2020</small>
- Introduce new feature learner: 
    - RelMTModel [now [RelMT](https://getml.com/latest/reference/feature_learning/relmt/)], 
    - RelMTTimeSeries [now integrated in [TimeSeries](https://getml.com/latest/reference/data/time_series/)]

### 0.12.0 <small>Oct 1, 2020</small>
- Extend dataframe handling: [delete()](https://getml.com/latest/reference/data/data_frame/#getml.data.DataFrame.delete), [exists()](https://getml.com/latest/reference/data/#getml.data.exists)
- Data set provisioning: [load_air_pollution()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_air_pollution), [load_atherosclerosis()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_atherosclerosis), [load_biodegradability()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_biodegradability), [load_consumer_expenditures()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_consumer_expenditures), [load_interstate94()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_interstate94), [load_loans()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_loans), [load_occupancy()](https://getml.com/latest/reference/datasets/datasets/#getml.datasets.load_occupancy)
- High-level hyperopt handlers: [tune_feature_learners()](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.tune_feature_learners), [tune_predictors()](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.tune_predictors)
- Improve pipeline functionality: [delete()](https://getml.com/latest/reference/pipeline/#getml.pipeline.delete), [exists()](https://getml.com/latest/reference/pipeline/#getml.pipeline.exists), [Columns](https://getml.com/latest/reference/pipeline/columns/)
- Introduce preprocessors: [EmailDomain](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.EmailDomain), [Imputation](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.Imputation), [Seasonal](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.Seasonal), [Substring](https://getml.com/latest/reference/preprocessors/#getml.preprocessors.Substring)

### 0.11.1 <small>Jul 13, 2020</small>
- Add pipeline functionality: [Pipeline](https://getml.com/latest/reference/pipeline/pipeline/), [list_pipelines()](https://getml.com/latest/reference/pipeline/#getml.pipeline.list_pipelines), [Features](https://getml.com/latest/reference/pipeline/features/), [Metrics](https://getml.com/latest/reference/pipeline/metrics/), [SQLCode](https://getml.com/latest/reference/pipeline/sql_code/), [Scores](https://getml.com/latest/reference/pipeline/scores_container/)
- Better control of hyperparameter optimization: [burn_in](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.burn_in), [kernels](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.kernels), [optimization](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.optimization)
- Handling of time stamps: [time](https://getml.com/latest/reference/data/time/)
- Improve database I/O: [connect_odbc()](https://getml.com/latest/reference/database/database/#getml.database.connect_odbc.connect_odbc), [copy_table()](https://getml.com/latest/reference/database/database/#getml.database.copy_table.copy_table), [list_connections()](https://getml.com/latest/reference/database/database/#getml.database.list_connections.list_connections), [read_s3()](https://getml.com/latest/reference/data/data_frame/#getml.data.DataFrame.read_s3), [sniff_s3()](https://getml.com/latest/reference/database/database/#getml.database.sniff_s3.sniff_s3)
- Enable S3 access: [set_s3_access_key_id()](https://getml.com/latest/reference/data/access/#getml.data.access.set_s3_access_key_id), [set_s3_secret_access_key()](https://getml.com/latest/reference/data/access/#getml.data.access.set_s3_secret_access_key)
- New Feature Learner: MultirelTimeSeries, RelboostTimeSeries [now both integrated in [TimeSeries](https://getml.com/latest/reference/data/time_series/)]

### 0.10.0 <small>Mar 17, 2020</small>
- Add [XGBoostClassifier](https://getml.com/latest/reference/predictors/xgboost_classifier/) and [XGBoostRegressor](https://getml.com/latest/reference/predictors/xgboost_regressor/) for improved predictive power
- Overhaul of documentation 
    - Introduction of "getML in one minute" (now [Quickstart](https://getml.com/latest/user_guide/quick_start/)) and "How to use this guide" (now [User Guide](https://getml.com/latest/user_guide/))
    - Introduction of User Guide (now [Concepts](https://getml.com/latest/user_guide/concepts/)) to include data annotation, feature engineering, hyperparameter optimization and more
- Integration with additional databases like [Greenplum](https://getml.com/latest/reference/database/database/#getml.database.connect_greenplum.connect_greenplum), [MariaDB](https://getml.com/latest/reference/database/database/#getml.database.connect_mariadb.connect_mariadb), [MySQL](https://getml.com/latest/reference/database/database/#getml.database.connect_mysql.connect_mysql), and extended [PostgreSQL](https://getml.com/latest/reference/database/database/#getml.database.connect_postgres.connect_postgres) support

### 0.9.1	<small>Mar 17, 2020</small>
- Include hotfix for new domain getml.com

### 0.9	<small>Dec 9, 2019</small>
- Rework hyperopt design and handling, added [load_hyperopt()](https://getml.com/latest/reference/hyperopt/#getml.hyperopt.load_hyperopt.load_hyperopt)
- Improved dataframe handling: add [to_placeholder()](https://getml.com/latest/reference/data/data_frame/#getml.data.DataFrame.to_placeholder) and [nrows()](https://getml.com/latest/reference/data/data_frame/#getml.data.DataFrame.nrows)

### 0.8	<small>Oct 22, 2019</small>
- Rename Autosql to [Multirel](https://getml.com/latest/reference/feature_learning/multirel/)
- Boolean and categorical columns: Add support for boolean columns and operators, along with enhanced categorical column handling.
- Introduce API improvements: fitting, saving/loading of models, data transformation
- Add support for various aggregation functions such as [MEDIAN](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.MEDIAN), [VAR](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.VAR), [STDDEV](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.STDDEV), and [COUNT_DISTINCT](https://getml.com/latest/reference/feature_learning/aggregations/#getml.feature_learning.aggregations.COUNT_DISTINCT)
- Move from closed beta to [pip](https://pypi.org/project/getml/)
- Introduce basic hyperopt algorithms: [LatinHypercubeSearch](https://getml.com/latest/reference/hyperopt/latin/), [RandomSearch](https://getml.com/latest/reference/hyperopt/random/)
