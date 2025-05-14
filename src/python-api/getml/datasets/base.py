# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Load preprocessed datasets
"""

import json
import warnings
from typing import Dict, List, Optional, Tuple, Union
from urllib.request import urlopen

import pandas as pd

from getml.data import DataFrame
from getml.database.helpers import _retrieve_url

DataFrameT = Union[DataFrame, pd.DataFrame]
"""
DataFrame types for builtin demonstration datasets
"""

VERSION: str = "v1"

BUCKET: str = "https://static.getml.com/datasets"
"""S3 bucket containing the CSV files"""

ASSETS: Dict[str, List[str]] = {
    "air_pollution": ["population"],
    "atherosclerosis": ["population", "contr"],
    "biodegradability": [
        "molecule_train",
        "molecule_test",
        "molecule_validation",
        "atom",
        "bond",
        "gmember",
        "group",
    ],
    "consumer_expenditures": [
        "population_training",
        "population_testing",
        "population_validation",
        "expd",
        "fmld",
        "memd",
    ],
    "interstate94": ["traffic"],
    "loans": [
        "population_train",
        "population_test",
        "order",
        "trans",
        "meta",
    ],
    "loans_new": [
        "account",
        "card",
        "client",
        "disp",
        "district",
        "loan",
        "order",
        "trans",
    ],
    "occupancy": ["population_train", "population_test", "population_validation"],
}


def _load_dataset(
    ds_name: str,
    assets: Optional[List[str]] = None,
    roles: bool = False,
    units: bool = False,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """Helper function to load a dataset

    Args:
        ds_name:
            name of the dataset

        assets:
            CSV files to be loaded from the S3 bucket

        roles:
            Return getml.DataFrame with roles set

        units:
            Return getml.DataFrame with units set

        as_pandas:
            Return data as `pandas.DataFrame` s

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
            * Tuple containing (sorted alphabetically by `df.name`) the data as [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas` is True) or

            * Dict containing the with (`df.name` as keys and `df` as values) as [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas` is True) if `as_dict` is `True`.
    """
    base = f"{BUCKET}/{VERSION}/{ds_name}/preprocessed"

    if assets is None:
        assets = ASSETS[ds_name]

    if roles:
        filename = base + "/" + ds_name + "_" + "roles.json"
        response = urlopen(filename)
        roles_ = json.loads(response.read())
    else:
        roles_ = {}

    dfs = {}
    for asset in assets:
        asset_url = base + "/" + ds_name + "_" + asset + ".csv"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            csv_file = _retrieve_url(asset_url, description=asset)
            dfs[asset] = DataFrame.from_csv(
                name=asset,
                fnames=csv_file,
                roles=roles_.get(asset),
                verbose=False,
            )

    if units:
        filename = base + "/" + ds_name + "_" + "units.json"
        response = urlopen(filename)
        units_ = json.loads(response.read())

        for key, df in dfs.items():
            for unit, columns in units_.get(key, {}).items():
                if unit:
                    df.set_unit(columns, unit)

    if as_pandas:
        dfs = {key: df.to_pandas() for key, df in dfs.items()}

    if as_dict:
        return dfs

    return tuple(dfs.values())


def load_air_pollution(
    roles: bool = True,
    as_pandas: bool = False,
) -> DataFrameT:
    """
    Regression dataset on air pollution in Beijing, China

    The dataset consists of a single table split into train and test sets
    around 2014-01-01.

    !!! abstract "Reference"
        Liang, X., Zou, T., Guo, B., Li, S., Zhang, H., Zhang, S., Huang, H. and
        Chen, S. X. (2015). Assessing Beijing's PM2.5 pollution: severity, weather
        impact, APEC and winter heating. Proceedings of the Royal Society A, 471,
        20150257.

    Args:
        as_pandas:
            Return data as `pandas.DataFrame`

        roles:
            Return data with roles set

    Returns:
        A DataFrame holding the data described above.

            The following DataFrames are returned:

            * `air_pollution`

    ??? example
        ```python
        air_pollution = getml.datasets.load_air_pollution()
        type(air_pollution)
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the atherosclerosis dataset including all necessary
        preprocessing steps please refer to [getml-demo
        ](https://github.com/getml/getml-demo/blob/master/air_pollution.ipynb).


    Note:
        Roles can be set ad-hoc by supplying the respective flag. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. This dataset contains no units.
        Before using them in an analysis, a data model needs to be constructed
        using [`Placeholder`][getml.data.Placeholder].

    """

    ds_name = "air_pollution"

    dataset = _load_dataset(
        ds_name=ds_name,
        roles=roles,
        as_pandas=as_pandas,
    )
    assert isinstance(dataset, tuple), "Expected a tuple"
    return dataset[0]


def load_atherosclerosis(
    roles: bool = True,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """
    Binary classification dataset on the lethality of atherosclerosis

    The atherosclerosis dataset is a medical dataset from the [Relational Dataset Repository (former CTU Prague
    Relational Learning Repository)
    ](https://relational-data.org/dataset/Atherosclerosis). It contains
    information from a longitudinal study on 1417 middle-aged men observed over
    the course of 20 years. After preprocessing, it consists of 2 tables with 76
    and 66 columns:

    * `population`: Data on the study's participants

    * `contr`: Data on control dates

    The population table is split into a training (70%), a testing (15%) set and a
    validation (15%) set.

    Args:
        as_pandas:
            Return data as `pandas.DataFrame` s

        roles:
            Return data with roles set

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
        Tuple containing (sorted alphabetically by `df.name`) the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True) or
            if `as_dict` is `True`: Dictionary containing the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True). The keys correspond to the name of the DataFrame on the
            [`engine`][getml.engine].

            The following DataFrames are returned:

            - `population`
            - `contr`

    ??? example
        ```python
        population, contr = getml.datasets.load_atherosclerosis()
        type(population)
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the atherosclerosis dataset including all necessary
        preprocessing steps please refer to [getml-examples
        ](https://github.com/getml/getml-demo/blob/master/atherosclerosis.ipynb).


    Note:
        Roles can be set ad-hoc by supplying the respective flag. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. This dataset contains no units.
        Before using them in an analysis, a data model needs to be constructed
        using [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "atherosclerosis"

    return _load_dataset(
        ds_name=ds_name,
        roles=roles,
        as_pandas=as_pandas,
        as_dict=as_dict,
    )


def load_biodegradability(
    roles: bool = True,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """
    Regression dataset on molecule weight prediction

    The QSAR biodegradation dataset was built in the Milano Chemometrics and
    QSAR Research Group (Universita degli Studi Milano-Bicocca, Milano, Italy).
    The data have been used to develop QSAR (Quantitative Structure Activity
    Relationships) models for the study of the relationships between chemical
    structure and biodegradation of molecules. Biodegradation experimental
    values of 1055 chemicals were collected from the webpage of the National
    Institute of Technology and Evaluation of Japan (NITE).

    !!! abstract "Reference"
        Mansouri, K., Ringsted, T., Ballabio, D., Todeschini, R., Consonni, V.
        (2013). Quantitative Structure - Activity Relationship models for ready
        biodegradability of chemicals. Journal of Chemical Information and Modeling,
        53, 867-878

    The dataset was collected through the [Relational Dataset Repository (former CTU Prague
    Relational Learning Repository)](https://relational-data.org/dataset/Biodegradability)

    It contains information on 1309 molecules with 6166 bonds. It consists of 5
    tables.

    The population table is split into a training (50 %) and a testing (25%) and
    validation (25%) sets.

    Args:
        as_pandas:
            Return data as `pandas.DataFrame`

        roles:
            Return data with roles set

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
        Tuple containing (sorted alphabetically by `df.name`) the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True) or if `as_dict` is `True`: Dictionary containing the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True). The keys correspond to the name of the DataFrame on the
            [`engine`][getml.engine].

            The following DataFrames are returned:

            * `molecule`
            * `atom`
            * `bond`
            * `gmember`
            * `group`

    ??? example
        ```python
        biodegradability = getml.datasets.load_biodegradability(as_dict=True)
        type(biodegradability["molecule_train"])
        getml.data.data_frame.DataFrame
        ```

    Note:
        Roles can be set ad-hoc by supplying the respective flag. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. This dataset contains no units.
        Before using them in an analysis, a data model needs to be constructed
        using [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "biodegradability"

    return _load_dataset(
        ds_name=ds_name,
        roles=roles,
        as_pandas=as_pandas,
        as_dict=as_dict,
    )


def load_consumer_expenditures(
    roles: bool = True,
    units: bool = True,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """
    Binary classification dataset on consumer expenditures

    The Consumer Expenditure Data Set is a public domain data set provided by
    the [American Bureau of Labor Statistics](https://www.bls.gov/cex/pumd.htm).
    It includes the diary entries, where American consumers are asked to keep
    record of the products they have purchased each month.

    We use this dataset to classify whether an item was purchased as a gift or not.

    Args:
        roles:
            Return data with roles set

        units:
            Return data with units set

        as_pandas:
            Return data as `pandas.DataFrame`

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
        Tuple containing (sorted alphabetically by `df.name`) the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True) or
            if `as_dict` is `True`: Dictionary containing the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True). The keys correspond to the name of the DataFrame on the
            [`engine`][getml.engine].

            The following DataFrames are returned:

            * `population`
            * `expd`
            * `fmld`
            * `memd`

    ??? example
        ```python
        ce = getml.datasets.load_consumer_expenditures(as_dict=True)
        type(ce["expd"])
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the occupancy dataset including all necessary
        preprocessing steps please refer to [getml-examples
        ](https://github.com/getml/getml-demo/blob/master/consumer_expenditures.ipynb).

    Note:
        Roles and units can be set ad-hoc by supplying the respective flag. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float].
        Before using them in an analysis, a data model needs to be constructed
        using [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "consumer_expenditures"

    return _load_dataset(
        ds_name=ds_name,
        roles=roles,
        units=units,
        as_pandas=as_pandas,
        as_dict=as_dict,
    )


def load_interstate94(
    roles: bool = True,
    units: bool = True,
    as_pandas: bool = False,
) -> DataFrameT:
    """
    Regression dataset on traffic volume prediction

    The interstate94 dataset is a multivariate time series containing the
    hourly traffic volume on I-94 westbound from Minneapolis-St Paul. It is
    based on data provided by the
    [MN Department of Transportation](https://www.dot.state.mn.us/).
    Some additional data preparation done by
    [John Hogue](https://github.com/dreyco676/Anomaly_Detection_A_to_Z/). The
    dataset features some particular interesting characteristics common for
    time series, which classical models may struggle to appropriately deal
    with. Such characteristics are:

    * High frequency (hourly)
    * Dependence on irregular events (holidays)
    * Strong and overlapping cycles (daily, weekly)
    * Anomalies
    * Multiple seasonalities

    Args:
        roles:
            Return data with roles set

        units:
            Return data with units set

        as_pandas:
            Return data as `pandas.DataFrame`

    Returns:
        A DataFrame holding the data described above.

            The following DataFrames are returned:

            * `traffic`

    ??? example
        ```python
        traffic = getml.datasets.load_interstate94()
        type(traffic)
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the interstate94 dataset including all necessary
        preprocessing steps please refer to [getml-examples](https://github.com/getml/getml-demo/blob/master/interstate94.ipynb).

    Note:
        Roles and units can be set ad-hoc by supplying the respective flags. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. Before using them in an
        analysis, a data model needs to be constructed using
        [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "interstate94"
    dataset = _load_dataset(
        ds_name=ds_name,
        roles=roles,
        units=units,
        as_pandas=as_pandas,
    )
    assert isinstance(dataset, tuple), "Expected a tuple"
    return dataset[0]


def load_loans(
    roles: bool = True,
    units: bool = True,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """
    Binary classification dataset on loan default

    The loans dataset is based on a financial dataset from the [Relational Dataset Repository (former CTU Prague
    Relational Learning Repository)](https://relational-data.org/dataset/Financial).

    !!! abstract "Reference"
        Berka, Petr (1999). Workshop notes on Discovery Challenge PKDD'99.

    The dataset contains information on 606 successful and 76 unsuccessful
    loans. After some preprocessing it contains 5 tables

    * `account`: Information about the borrower(s) of a given loan.

    * `loan`: Information about the loans themselves, such as the date of creation, the amount, and the planned duration of the loan. The target variable is the status of the loan (default/no default)

    * `meta`: Meta information about the obligor, such as gender and geo-information

    * `order`: Information about permanent orders, debited payments and account balances.

    * `trans`: Information about transactions and accounts balances.

    The population table is split into a training and a testing set at 80% of the main population.

    Args:
        roles:
            Return data with roles set

        units:
            Return data with units set

        as_pandas:
            Return data as `pandas.DataFrame`

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
        Tuple containing (sorted alphabetically by `df.name`) the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True) or
            if `as_dict` is `True`: Dictionary containing the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True). The keys correspond to the name of the DataFrame on the
            [`engine`][getml.engine].

            The following DataFrames are returned:

            * `account`
            * `loan`
            * `meta`
            * `order`
            * `trans`

    ??? example
        ```python
        loans = getml.datasets.load_loans(as_dict=True)
        type(loans["population_train"])
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the loans dataset including all necessary
        preprocessing steps please refer to [getml-examples
        ](https://github.com/getml/getml-demo/blob/master/loans.ipynb).

    Note:
        Roles and units can be set ad-hoc by supplying the respective flags. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. Before using them in an
        analysis, a data model needs to be constructed using
        [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "loans"

    return _load_dataset(
        ds_name=ds_name,
        roles=roles,
        units=units,
        as_pandas=as_pandas,
        as_dict=as_dict,
    )


def load_occupancy(
    roles: bool = True,
    as_pandas: bool = False,
    as_dict: bool = False,
) -> Union[Tuple[DataFrameT, ...], Dict[str, DataFrameT]]:
    """
    Binary classification dataset on occupancy detection

    The occupancy detection dataset is a very simple multivariate time series
    from the [UCI Machine Learning Repository
    ](https://archive.ics.uci.edu/dataset/357/occupancy+detection). It is a
    binary classification problem. The task is to predict room occupancy
    from Temperature, Humidity, Light and CO2.

    !!! abstract "Reference"
        Candanedo, L. M., & Feldheim, V. (2016). Accurate occupancy detection of an
        office room from light, temperature, humidity and CO2 measurements using
        statistical learning models. Energy and Buildings, 112, 28-39.

    Args:
        roles:
            Return data with roles set

        as_pandas:
            Return data as `pandas.DataFrame` s

        as_dict:
            Return data as dict with `df.name` as keys and
            `df` as values.

    Returns:
        Tuple containing (sorted alphabetically by `df.name`) the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True) or
            if `as_dict` is `True`: Dictionary containing the data as
            [`DataFrame`][getml.DataFrame] or `pandas.DataFrame` (if `as_pandas`
            is True). The keys correspond to the name of the DataFrame on the
            [`engine`][getml.engine].

            The following DataFrames are returned:

            * `population_train`
            * `population_test`
            * `population_validation`

    ??? example
        ```python
        population_train, population_test, _ = getml.datasets.load_occupancy()
        type(occupancy_train)
        getml.data.data_frame.DataFrame
        ```
        For a full analysis of the occupancy dataset including all necessary
        preprocessing steps please refer to [getml-examples
        ](https://github.com/getml/getml-demo/blob/master/occupancy.ipynb).


    Note:
        Roles can be set ad-hoc by supplying the respective flag. If
        `roles` is `False`, all columns in the returned
        [`DataFrame`][getml.data.DataFrame] have roles
        [`unused_string`][getml.data.roles.unused_string] or
        [`unused_float`][getml.data.roles.unused_float]. This dataset contains no units.
        Before using them in an analysis, a data model needs to be constructed
        using [`Placeholder`][getml.data.Placeholder].
    """

    ds_name = "occupancy"

    return _load_dataset(
        ds_name=ds_name,
        roles=roles,
        as_pandas=as_pandas,
        as_dict=as_dict,
    )
