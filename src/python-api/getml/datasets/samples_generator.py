# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""
Generate samples of artificial data sets.
"""

import datetime
from typing import Optional, Tuple

import numpy as np
import pandas as pd  # type: ignore

import getml.data as data
from getml.data.data_frame import DataFrame
from getml.feature_learning import aggregations

# -----------------------------------------------------------------------------


def _aggregate(
    table: pd.DataFrame, aggregation: str, col: str, join_key: str
) -> pd.DataFrame:
    """
    Implements the aggregation."""

    if aggregation == aggregations.Avg:
        return table[[col, join_key]].groupby([join_key], as_index=False).mean()

    if aggregation == aggregations.Count:
        return table[[col, join_key]].groupby([join_key], as_index=False).count()

    if aggregation == aggregations.CountDistinct:
        series = (
            table[[col, join_key]].groupby([join_key], as_index=False)[col].nunique()
        )

        output = table[[col, join_key]].groupby([join_key], as_index=False).count()

        output[col] = series

        return output

    if aggregation == aggregations.CountMinusCountDistinct:
        series = (
            table[[col, join_key]].groupby([join_key], as_index=False)[col].nunique()
        )

        output = table[[col, join_key]].groupby([join_key], as_index=False).count()

        output[col] -= series

        return output

    if aggregation == aggregations.Max:
        return table[[col, join_key]].groupby([join_key], as_index=False).max()

    if aggregation == aggregations.Median:
        return table[[col, join_key]].groupby([join_key], as_index=False).median()

    if aggregation == aggregations.Min:
        return table[[col, join_key]].groupby([join_key], as_index=False).min()

    if aggregation == aggregations.Stddev:
        return table[[col, join_key]].groupby([join_key], as_index=False).std()

    if aggregation == aggregations.Sum:
        return table[[col, join_key]].groupby([join_key], as_index=False).sum()

    if aggregation == aggregations.Var:
        return table[[col, join_key]].groupby([join_key], as_index=False).var()

    raise Exception("Aggregation '" + aggregation + "' not known!")


# -----------------------------------------------------------------------------


def make_categorical(
    n_rows_population: int = 500,
    n_rows_peripheral: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name: str = "",
    aggregation: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate a random dataset with categorical variables

        The dataset consists of a population table and one peripheral table.

        The peripheral table has 3 columns:

        * `column_01`: random categorical variable between '0' and '9'
        * `join_key`: random integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1

        The population table has 4 columns:

        * `column_01`: random categorical variable between '0' and '9'
        * `join_key`: unique integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1
        * `targets`: target variable. Defined as the number of matching entries in
          the peripheral table for which ``time_stamp_peripheral <
          time_stamp_population`` and the category in the peripheral table is not
          1, 2 or 9. The SQL definition of the target variable read like this

        .. code-block:: sql

            SELECT aggregation( column_01 )
            FROM POPULATION_TABLE t1
            LEFT JOIN PERIPHERAL_TABLE t2
            ON t1.join_key = t2.join_key
            WHERE (
               ( t2.column_01 != '1' AND t2.column_01 != '2' AND t2.column_01 != '9' )
            ) AND t2.time_stamps <= t1.time_stamps
            GROUP BY t1.join_key,
                 t1.time_stamp;

        Args:
            n_rows_population (int, optional):
                Number of rows in the population table.

            n_row_peripheral (int, optional):
                Number of rows in the peripheral table.

            random_state (Optional[int], optional):
                Seed to initialize the random number generator used for
                the dataset creation. If set to None, the seed will be the
                'microsecond' component of
                :py:func:`datetime.datetime.now()`.

            population_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the population
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `categorical_population_` and the seed of the random
                number generator.

            peripheral_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the peripheral
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `categorical_peripheral_` and the seed of the random
                number generator.

            aggregation(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column.

        Returns:
            tuple:
                tuple containing:

                    * population (:class:`getml.DataFrame`): Population table

                    * peripheral (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101
    population_table = pd.DataFrame()
    population_table["column_01"] = random.randint(0, 10, n_rows_population).astype(str)
    population_table["join_key"] = np.arange(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01"] = random.randint(0, 10, n_rows_peripheral).astype(str)
    peripheral_table["join_key"] = random.randint(
        0, n_rows_population, n_rows_peripheral
    )
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral)

    # Compute targets
    temp = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral"] <= temp["time_stamp_population"])
        & (temp["column_01"] != "1")
        & (temp["column_01"] != "2")
        & (temp["column_01"] != "9")
    ]

    # Define the aggregation
    temp = _aggregate(temp, aggregation, "column_01", "join_key")

    temp = temp.rename(index=str, columns={"column_01": "targets"})

    population_table = population_table.merge(temp, how="left", on="join_key")

    del temp

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table.targets = np.where(
        np.isnan(population_table["targets"]), 0, population_table["targets"]
    )

    # Set default names if none where provided.
    if not population_name:
        population_name = "categorical_population_" + str(random_state)
    if not peripheral_name:
        peripheral_name = "categorical_peripheral_" + str(random_state)

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "categorical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name,
        roles={
            "join_key": ["join_key"],
            "categorical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    return population_on_engine, peripheral_on_engine


# -----------------------------------------------------------------------------


def make_discrete(
    n_rows_population: int = 500,
    n_rows_peripheral: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name: str = "",
    aggregation: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate a random dataset with categorical variables

        The dataset consists of a population table and one peripheral table.

        The peripheral table has 3 columns:

        * `column_01`: random integer between -10 and 10
        * `join_key`: random integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1

        The population table has 4 columns:

        * `column_01`: random number between -1 and 1
        * `join_key`: unique integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1
        * `targets`: target variable. Defined as the minimum value greater than 0
          in the peripheral table for which
          ``time_stamp_peripheral < time_stamp_population``
          and the join key matches

        .. code-block:: sql

            SELECT aggregation( column_01 )
            FROM POPULATION t1
            LEFT JOIN PERIPHERAL t2
            ON t1.join_key = t2.join_key
            WHERE (
               ( t2.column_01 > 0 )
            ) AND t2.time_stamp <= t1.time_stamp
            GROUP BY t1.join_key,
                     t1.time_stamp;

        Args:
            n_rows_population (int, optional):
                Number of rows in the population table.

            n_row_peripheral (int, optional):
                Number of rows in the peripheral table.

            random_state (Optional[int], optional):
                Seed to initialize the random number generator used for
                the dataset creation. If set to None, the seed will be the
                'microsecond' component of
                :py:func:`datetime.datetime.now()`.

            population_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the population
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `discrete_population_` and the seed of the random
                number generator.

            peripheral_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the peripheral
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `discrete_peripheral_` and the seed of the random
                number generator.

            aggregation(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column.

        Returns:
            tuple:
                tuple containing:

                    * population (:class:`getml.DataFrame`): Population table

                    * peripheral (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101

    population_table = pd.DataFrame()
    population_table["column_01"] = random.randint(0, 10, n_rows_population).astype(str)
    population_table["join_key"] = np.arange(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01"] = random.randint(-11, 11, n_rows_peripheral)
    peripheral_table["join_key"] = random.randint(
        0, n_rows_population, n_rows_peripheral
    )
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral)

    # Compute targets
    temp = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral"] <= temp["time_stamp_population"])
        & (temp["column_01"] > 0.0)
    ]

    # Define the aggregation
    temp = _aggregate(temp, aggregation, "column_01", "join_key")

    temp = temp.rename(index=str, columns={"column_01": "targets"})

    population_table = population_table.merge(temp, how="left", on="join_key")

    del temp

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table.targets = np.where(
        np.isnan(population_table["targets"]), 0, population_table["targets"]
    )

    # Set default names if none where provided.
    if not population_name:
        population_name = "discrete_population_" + str(random_state)
    if not peripheral_name:
        peripheral_name = "discrete_peripheral_" + str(random_state)

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    return population_on_engine, peripheral_on_engine


# -----------------------------------------------------------------------------


def make_numerical(
    n_rows_population: int = 500,
    n_rows_peripheral: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name: str = "",
    aggregation: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate a random dataset with continous numerical variables

        The dataset consists of a population table and one peripheral table.

        The peripheral table has 3 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: random integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1

        The population table has 4 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: unique integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1
        * `targets`: target variable. Defined as the number of matching entries in
          the peripheral table for which ``time_stamp_peripheral <
          time_stamp_population < time_stamp_peripheral + 0.5``

        .. code-block:: sql

            SELECT aggregation( column_01 )
            FROM POPULATION t1
            LEFT JOIN PERIPHERAL t2
            ON t1.join_key = t2.join_key
            WHERE (
               ( t1.time_stamp - t2.time_stamp <= 0.5 )
            ) AND t2.time_stamp <= t1.time_stamp
            GROUP BY t1.join_key,
                 t1.time_stamp;

        Args:
            n_rows_population (int, optional):
                Number of rows in the population table.

            n_row_peripheral (int, optional):
                Number of rows in the peripheral table.

            random_state (Optional[int], optional):
                Seed to initialize the random number generator used for
                the dataset creation. If set to None, the seed will be the
                'microsecond' component of
                :py:func:`datetime.datetime.now()`.

            population_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the population
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `numerical_population_` and the seed of the random
                number generator.

            peripheral_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the peripheral
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `numerical_peripheral_` and the seed of the random
                number generator.

            aggregation(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column.

        Returns:
            tuple:
                tuple containing:

                    * population (:class:`getml.DataFrame`): Population table

                    * peripheral (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101

    population_table = pd.DataFrame()
    population_table["column_01"] = random.rand(n_rows_population) * 2.0 - 1.0
    population_table["join_key"] = np.arange(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01"] = random.rand(n_rows_peripheral) * 2.0 - 1.0
    peripheral_table["join_key"] = random.randint(
        0, n_rows_population, n_rows_peripheral
    )
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral)

    # Compute targets
    temp = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral"] <= temp["time_stamp_population"])
        & (temp["time_stamp_peripheral"] >= temp["time_stamp_population"] - 0.5)
    ]

    # Define the aggregation
    temp = _aggregate(temp, aggregation, "column_01", "join_key")

    temp = temp.rename(index=str, columns={"column_01": "targets"})

    population_table = population_table.merge(temp, how="left", on="join_key")

    del temp

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table.targets = np.where(
        np.isnan(population_table["targets"]), 0, population_table["targets"]
    )

    # Set default names if none where provided.
    if not population_name:
        population_name = "numerical_population_" + str(random_state)
    if not peripheral_name:
        peripheral_name = "numerical_peripheral_" + str(random_state)

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    return population_on_engine, peripheral_on_engine


# -----------------------------------------------------------------------------


def make_same_units_categorical(
    n_rows_population: int = 500,
    n_rows_peripheral: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name: str = "",
    aggregation: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate a random dataset with categorical variables

    The dataset consists of a population table and one peripheral table.

    The peripheral table has 3 columns:

    * `column_01`: random categorical variable between '0' and '9'
    * `join_key`: random integer in the range from 0 to ``n_rows_population``
    * `time_stamp`: random number between 0 and 1

    The population table has 4 columns:

    * `column_01`: random categorical variable between '0' and '9'
    * `join_key`: unique integer in the range from 0 to ``n_rows_population``
    * `time_stamp`: random number between 0 and 1
    * `targets`: target variable. Defined as the number of matching entries in
      the peripheral table for which ``time_stamp_peripheral <
      time_stamp_population`` and the category in the peripheral table is not
      1, 2 or 9

    .. code-block:: sql

        SELECT aggregation( column_02 )
        FROM POPULATION_TABLE t1
        LEFT JOIN PERIPHERAL_TABLE t2
        ON t1.join_key = t2.join_key
        WHERE (
           ( t1.column_01 == t2.column_01 )
        ) AND t2.time_stamps <= t1.time_stamps
        GROUP BY t1.join_key,
             t1.time_stamp;

    Args:
        n_rows_population (int, optional):
            Number of rows in the population table.

        n_row_peripheral (int, optional):
            Number of rows in the peripheral table.

        random_state (Optional[int], optional):
            Seed to initialize the random number generator used for
            the dataset creation. If set to None, the seed will be the
            'microsecond' component of
            :py:func:`datetime.datetime.now()`.

        population_name (string, optional):
            Name assigned to the create
            :class:`~getml.DataFrame` holding the population
            table. If set to a name already existing on the getML
            engine, the corresponding :class:`~getml.DataFrame`
            will be overwritten. If set to an empty string, a unique
            name will be generated by concatenating
            `make_same_units_categorical_population_` and the seed of the random
            number generator.

        peripheral_name (string, optional):
            Name assigned to the create
            :class:`~getml.DataFrame` holding the peripheral
            table. If set to a name already existing on the getML
            engine, the corresponding :class:`~getml.DataFrame`
            will be overwritten. If set to an empty string, a unique
            name will be generated by concatenating
            `make_same_units_categorical_peripheral_` and the seed of the random
            number generator.

        aggregation(string, optional):
            :mod:`~getml.models.aggregations` used to generate the 'target'
            column.

    Returns:
        tuple:
            tuple containing:

                * population (:class:`getml.DataFrame`): Population table

                * peripheral (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101

    population_table = pd.DataFrame()
    population_table["column_01_population"] = (
        (random.rand(n_rows_population) * 10.0).astype(np.int32).astype(str)
    )
    population_table["join_key"] = range(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01_peripheral"] = (
        (random.rand(n_rows_peripheral) * 10.0).astype(np.int32).astype(str)
    )
    peripheral_table["column_02"] = random.rand(n_rows_peripheral) * 2.0 - 1.0
    peripheral_table["join_key"] = [
        int(float(n_rows_population) * random.rand(1)[0])
        for i in range(n_rows_peripheral)
    ]
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral)

    # ----------------

    temp = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population", "column_01_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral"] <= temp["time_stamp_population"])
        & (temp["column_01_peripheral"] == temp["column_01_population"])
    ]

    # Define the aggregation
    temp = _aggregate(temp, aggregation, "column_02", "join_key")

    temp = temp.rename(index=str, columns={"column_02": "targets"})

    population_table = population_table.merge(temp, how="left", on="join_key")

    population_table = population_table.rename(
        index=str, columns={"column_01_population": "column_01"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"column_01_peripheral": "column_01"}
    )

    del temp

    # ----------------

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    # ----------------

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table["targets"] = [
        0.0 if val != val else val for val in population_table["targets"]
    ]

    # ----------------

    # Set default names if none where provided.
    population_name = (
        population_name
        or "make_same_units_categorical_population__" + str(random_state)
    )

    peripheral_name = (
        peripheral_name
        or "make_same_units_categorical_peripheral__" + str(random_state)
    )

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "categorical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name,
        roles={
            "join_key": ["join_key"],
            "categorical": ["column_01"],
            "numerical": ["column_02"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    # ----------------

    return population_on_engine, peripheral_on_engine


# -----------------------------------------------------------------------------


def make_same_units_numerical(
    n_rows_population: int = 500,
    n_rows_peripheral: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name: str = "",
    aggregation: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame]:
    """
    Generate a random dataset with continous numerical variables

        The dataset consists of a population table and one peripheral table.

        The peripheral table has 3 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: random integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1

        The population table has 4 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: unique integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1
        * `targets`: target variable. Defined as the number of matching entries in
          the peripheral table for which ``time_stamp_peripheral <
          time_stamp_population < time_stamp_peripheral + 0.5``

        .. code-block:: sql

            SELECT aggregation( column_01 )
            FROM POPULATION t1
            LEFT JOIN PERIPHERAL t2
            ON t1.join_key = t2.join_key
            WHERE (
               ( t1.column_01 - t2.column_01 <= 0.5 )
            ) AND t2.time_stamp <= t1.time_stamp
            GROUP BY t1.join_key,
                 t1.time_stamp;

        Args:
            n_rows_population (int, optional):
                Number of rows in the population table.

            n_row_peripheral (int, optional):
                Number of rows in the peripheral table.

            random_state (Union[int, None], optional):
                Seed to initialize the random number generator used for
                the dataset creation. If set to None, the seed will be the
                'microsecond' component of
                :py:func:`datetime.datetime.now()`.

            population_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the population
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `make_same_units_numerical_population_` and the seed of the random
                number generator.

            peripheral_name (string, optional):
                Name assigned to readcreate
                :class:`~getml.DataFrame` holding the peripheral
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `make_same_units_numerical_peripheral_` and the seed of the random
                number generator.

            aggregation(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column.

        Returns:
            tuple:
                tuple containing:

                    * population (:class:`getml.DataFrame`): Population table

                    * peripheral (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101

    population_table = pd.DataFrame()
    population_table["column_01_population"] = (
        random.rand(n_rows_population) * 2.0 - 1.0
    )
    population_table["join_key"] = range(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01_peripheral"] = (
        random.rand(n_rows_peripheral) * 2.0 - 1.0
    )
    peripheral_table["join_key"] = [
        int(float(n_rows_population) * random.rand(1)[0])
        for i in range(n_rows_peripheral)
    ]
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral)

    # ----------------

    temp = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population", "column_01_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral"] <= temp["time_stamp_population"])
        & (temp["column_01_peripheral"] > temp["column_01_population"] - 0.5)
    ]

    # Define the aggregation
    temp = (
        temp[["column_01_peripheral", "join_key"]]
        .groupby(["join_key"], as_index=False)
        .count()
    )

    temp = temp.rename(index=str, columns={"column_01_peripheral": "targets"})

    population_table = population_table.merge(temp, how="left", on="join_key")

    population_table = population_table.rename(
        index=str, columns={"column_01_population": "column_01"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"column_01_peripheral": "column_01"}
    )

    del temp

    # ----------------

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    # ----------------

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table["targets"] = [
        0.0 if val != val else val for val in population_table["targets"]
    ]

    # ----------------

    # Set default names if none where provided.
    if not population_name:
        population_name = "same_unit_numerical_population_" + str(random_state)
    if not peripheral_name:
        peripheral_name = "same_unit_numerical_peripheral_" + str(random_state)

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    return population_on_engine, peripheral_on_engine


# -----------------------------------------------------------------------------


def make_snowflake(
    n_rows_population: int = 500,
    n_rows_peripheral1: int = 5000,
    n_rows_peripheral2: int = 125000,
    random_state: Optional[int] = None,
    population_name: str = "",
    peripheral_name1: str = "",
    peripheral_name2: str = "",
    aggregation1: str = aggregations.Sum,
    aggregation2: str = aggregations.Count,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Generate a random dataset with continous numerical variables

        The dataset consists of a population table and two peripheral tables.

        The first peripheral table has 4 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: random integer in the range from 0 to ``n_rows_population``
        * `join_key2`: unique integer in the range from 0 to ``n_rows_peripheral1``
        * `time_stamp`: random number between 0 and 1

        The second peripheral table has 3 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key2`: random integer in the range from 0 to ``n_rows_peripheral1``
        * `time_stamp`: random number between 0 and 1

        The population table has 4 columns:

        * `column_01`:  random number between -1 and 1
        * `join_key`: unique integer in the range from 0 to ``n_rows_population``
        * `time_stamp`: random number between 0 and 1
        * `targets`: target variable as defined by the SQL block below:

        .. code-block:: sql

            SELECT aggregation1( feature_1_1 )
            FROM POPULATION t1
            LEFT JOIN (
                SELECT aggregation2( t4.column_01 ) AS feature_1_1
                FROM PERIPHERAL t3
                LEFT JOIN PERIPHERAL2 t4
                ON t3.join_key2 = t4.join_key2
                WHERE (
                   ( t3.time_stamp - t4.time_stamp <= 0.5 )
                ) AND t4.time_stamp <= t3.time_stamp
                GROUP BY t3.join_key,
                     t3.time_stamp
            ) t2
            ON t1.join_key = t2.join_key
            WHERE t2.time_stamp <= t1.time_stamp
            GROUP BY t1.join_key,
                 t1.time_stamp;

        Args:
            n_rows_population (int, optional):
                Number of rows in the population table.

            n_row_peripheral1 (int, optional):
                Number of rows in the first peripheral table.

            n_row_peripheral2 (int, optional):
                Number of rows in the second peripheral table.

            random_state (Union[int, None], optional):
                Seed to initialize the random number generator used for
                the dataset creation. If set to None, the seed will be the
                'microsecond' component of
                :py:func:`datetime.datetime.now()`.

            population_name (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the population
                table. If set to a name already existing on the getML
                engine, the corresponding :class:`~getml.DataFrame`
                will be overwritten. If set to an empty string, a unique
                name will be generated by concatenating
                `snowflake_population_` and the seed of the random
                number generator.

            peripheral_name1 (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the first
                peripheral table. If set to a name already existing on the
                getML engine, the corresponding
                :class:`~getml.DataFrame` will be overwritten. If
                set to an empty string, a unique name will be generated by
                concatenating `snowflake_peripheral_1_` and the seed of the
                random number generator.

            peripheral_name2 (string, optional):
                Name assigned to the create
                :class:`~getml.DataFrame` holding the second
                peripheral table. If set to a name already existing on the
                getML engine, the corresponding
                :class:`~getml.DataFrame` will be overwritten. If
                set to an empty string, a unique name will be generated by
                concatenating `snowflake_peripheral_2_` and the seed of the
                random number generator.

            aggregation1(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column in the first peripheral table.

            aggregation2(string, optional):
                :mod:`~getml.models.aggregations` used to generate the 'target'
                column in the second peripheral table.

        Returns:
            tuple:
                tuple containing:

                    * population (:class:`getml.DataFrame`): Population table

                    * peripheral (:class:`getml.DataFrame`): Peripheral table

                    * peripheral_2 (:class:`getml.DataFrame`): Peripheral table
    """

    if random_state is None:
        random_state = datetime.datetime.now().microsecond

    random = np.random.RandomState(random_state)  # pylint: disable=E1101

    population_table = pd.DataFrame()
    population_table["column_01"] = random.rand(n_rows_population) * 2.0 - 1.0
    population_table["join_key"] = range(n_rows_population)
    population_table["time_stamp_population"] = random.rand(n_rows_population)

    peripheral_table = pd.DataFrame()
    peripheral_table["column_01"] = random.rand(n_rows_peripheral1) * 2.0 - 1.0
    peripheral_table["join_key"] = [
        int(float(n_rows_population) * random.rand(1)[0])
        for i in range(n_rows_peripheral1)
    ]
    peripheral_table["join_key2"] = range(n_rows_peripheral1)
    peripheral_table["time_stamp_peripheral"] = random.rand(n_rows_peripheral1)

    peripheral_table2 = pd.DataFrame()
    peripheral_table2["column_01"] = random.rand(n_rows_peripheral2) * 2.0 - 1.0
    peripheral_table2["join_key2"] = [
        int(float(n_rows_peripheral1) * random.rand(1)[0])
        for i in range(n_rows_peripheral2)
    ]
    peripheral_table2["time_stamp_peripheral2"] = random.rand(n_rows_peripheral2)

    # ----------------
    # Merge peripheral_table with peripheral_table2

    temp = peripheral_table2.merge(
        peripheral_table[["join_key2", "time_stamp_peripheral"]],
        how="left",
        on="join_key2",
    )

    # Apply some conditions
    temp = temp[
        (temp["time_stamp_peripheral2"] <= temp["time_stamp_peripheral"])
        & (temp["time_stamp_peripheral2"] >= temp["time_stamp_peripheral"] - 0.5)
    ]

    # Define the aggregation
    temp = _aggregate(temp, aggregation2, "column_01", "join_key2")

    temp = temp.rename(index=str, columns={"column_01": "temporary"})

    peripheral_table = peripheral_table.merge(temp, how="left", on="join_key2")

    del temp

    # Replace NaN with 0.0
    peripheral_table["temporary"] = [
        0.0 if val != val else val for val in peripheral_table["temporary"]
    ]

    # ----------------
    # Merge population_table with peripheral_table

    temp2 = peripheral_table.merge(
        population_table[["join_key", "time_stamp_population"]],
        how="left",
        on="join_key",
    )

    # Apply some conditions
    temp2 = temp2[(temp2["time_stamp_peripheral"] <= temp2["time_stamp_population"])]

    # Define the aggregation
    temp2 = _aggregate(temp2, aggregation1, "temporary", "join_key")

    temp2 = temp2.rename(index=str, columns={"temporary": "targets"})

    population_table = population_table.merge(temp2, how="left", on="join_key")

    del temp2

    # Replace NaN targets with 0.0 - target values may never be NaN!.
    population_table["targets"] = [
        0.0 if val != val else val for val in population_table["targets"]
    ]

    # Remove temporary column.
    del peripheral_table["temporary"]

    # ----------------

    population_table = population_table.rename(
        index=str, columns={"time_stamp_population": "time_stamp"}
    )

    peripheral_table = peripheral_table.rename(
        index=str, columns={"time_stamp_peripheral": "time_stamp"}
    )

    peripheral_table2 = peripheral_table2.rename(
        index=str, columns={"time_stamp_peripheral2": "time_stamp"}
    )

    # ----------------

    # Set default names if none where provided.
    if not population_name:
        population_name = "snowflake_population_" + str(random_state)
    if not peripheral_name1:
        peripheral_name1 = "snowflake_peripheral_1_" + str(random_state)
    if not peripheral_name2:
        peripheral_name2 = "snowflake_peripheral_2_" + str(random_state)

    # Create the data.DataFrame counterpart.
    population_on_engine = data.DataFrame(
        name=population_name,
        roles={
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
            "target": ["targets"],
        },
    ).read_pandas(population_table)

    peripheral_on_engine = data.DataFrame(
        name=peripheral_name1,
        roles={
            "join_key": ["join_key", "join_key2"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table)

    peripheral_on_engine2 = data.DataFrame(
        name=peripheral_name2,
        roles={
            "join_key": ["join_key2"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_pandas(peripheral_table2)

    # ----------------

    return population_on_engine, peripheral_on_engine, peripheral_on_engine2


# -----------------------------------------------------------------------------
