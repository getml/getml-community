import decimal
import os
from datetime import datetime, timezone
from inspect import cleandoc
from tempfile import NamedTemporaryFile

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import getml


@pytest.fixture
def parquet_file():
    table = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})
    with NamedTemporaryFile() as f:
        pq.write_table(table, f.name)
        yield f.name


@pytest.fixture
def csv_file():
    data = cleandoc(
        """
        name,column_01,join_key,time_stamp
        patrick,0,22,2019-01-01
        alex,1,23,2019-01-02
        phil,2, 24,2019-01-03
        ulrike,3,25,2019-01-04
        """
    )

    with NamedTemporaryFile(mode="w", delete=False) as csv_file:
        csv_file.write(data)
        csv_file.flush()
        yield csv_file.name
        os.remove(csv_file.name)


@pytest.fixture
def csv_file_custom_ts():
    data = cleandoc(
        """
        name,time_stamp
        patrick,date: 2019-01-01; time: 00:00:00
        alex,date: 2019-01-02; time: 00:00:00
        phil,date: 2019-01-03; time: 00:00:00
        ulrike,date: 2019-01-04; time: 00:00:00
        """
    )

    with NamedTemporaryFile(mode="w", delete=False) as csv_file:
        csv_file.write(data)
        csv_file.flush()
        yield csv_file.name
        os.remove(csv_file.name)


@pytest.fixture
def json_payload():
    payload = cleandoc(
        """
        {
            "colors": ["blue", "green", "red", "yellow"],
            "numbers": [2.4, 3.0, 1.2, 1.4]
        }
        """
    )
    return payload


@pytest.fixture
def pandas_df():
    random = np.random.RandomState(8290)
    pandas_df = pd.DataFrame()
    pandas_df["col"] = random.rand(100) * 2.0 - 1.0
    pandas_df["join_key"] = random.randint(1, 200, 100)
    pandas_df["time_stamp"] = [pd.Timestamp(ii) for ii in random.rand(100) * 1e15]
    return pandas_df


@pytest.fixture
def df1():
    json_str1 = """{
        "names": ["patrick", "alex", "phil", "ulrike"],
        "column_01": [2.4, 3.0, 1.2, 1.4],
        "join_key": ["0", "1", "2", "3"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04"]
    }"""
    my_df1 = getml.DataFrame(
        "MY DF 1",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_json(json_str1)
    return my_df1


@pytest.fixture
def df2():
    json_str2 = """{
        "names": ["patrick", "alex", "phil", "johannes", "ulrike", "adil"],
        "column_01": [2.4, 3.0, 1.2, 1.4, 3.4, 2.2],
        "join_key": ["0", "1", "2", "2", "3", "4"],
        "time_stamp": ["2019-01-01", "2019-01-02", "2019-01-03", "2019-01-04", "2019-01-05", "2019-01-06"]
    }"""

    my_df2 = getml.DataFrame(
        "MY DF 2",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
            "time_stamp": ["time_stamp"],
        },
    ).read_json(json_str2)

    return my_df2


@pytest.fixture
def df3():
    json_str = """{
        "names": ["patrick", "alex", "phil", "ulrike", "patrick", "alex", "phil", "ulrike", "NULL"],
        "column_01": [2.4, 3.0, 1.2, 1.4, 3.4, 2.2, 10.2, 13.5, 11.0],
        "join_key": ["0", "1", "2", "2", "3", "3", "4", "4", "4"]
    }"""

    my_df3 = getml.DataFrame(
        "MY DF3",
        roles={
            "categorical": ["names"],
            "join_key": ["join_key"],
            "numerical": ["column_01"],
        },
    ).read_json(json_str)
    return my_df3


@pytest.fixture
def duckdb_conn():
    conn = getml.database.connect_duckdb()
    yield conn


@pytest.fixture
def prepopulated_duckdb_conn(duckdb_conn):
    conn = duckdb_conn
    queries = (
        cleandoc(
            """
            CREATE TABLE Product (
                ProductID integer,
                MakeFlag integer,
                FinishedGoodsFlag integer,
                Name text,
                ProductNumber text
            );

            INSERT INTO Product
                VALUES (1, 0, 1, 'Adjust Headset', 'AD-100');

            INSERT INTO Product
                VALUES (2, 1, 0, 'Bearing Ball', 'BA-832');

            INSERT INTO Product
                VALUES (3, 0, 1, 'BB Ball Bearing', 'BE-234');

            INSERT INTO Product
                VALUES (4, 1, 0, 'Headset Ball Bearings', 'BE-290');

            INSERT INTO Product
                VALUES (5, 0, 1, 'Blade', 'BL-2036');
            """
        ),
        cleandoc(
            """
            CREATE TABLE SalesOrderDetail (
                SalesOrderID integer,
                SalesOrderDetailID integer,
                OrderQty integer,
                ProductID integer,
                UnitPrice text,
                UnitPriceDiscount text
            );

            INSERT INTO SalesOrderDetail
                VALUES (43659, 1, 1, 776, '2024.9940', '0.0000');

            INSERT INTO SalesOrderDetail
                VALUES (43659, 2, 3, 777, '2024.9940', '0.0000');

            INSERT INTO SalesOrderDetail
                VALUES (43659, 3, 1, 778, '2024.9940', '0.0000');

            INSERT INTO SalesOrderDetail
                VALUES (43659, 4, 1, 771, '2039.9940', '0.0000');

            INSERT INTO SalesOrderDetail
                VALUES (43660, 1, 1, 776, '2024.9940', '0.0000');
            """
        ),
        cleandoc(
            """
            CREATE TABLE Store (
                BusinessEntityID integer,
                Name text,
                Demographics text,
                rowguid text,
                ModifiedDate text
            );

            INSERT INTO Store
                VALUES (292, 'Store 292', 'demographics', 'guid', '2014-09-12 11:15:07.263');

            INSERT INTO Store
                VALUES (293, 'Store 293', 'demographics', 'guid', '2014-09-12 11:15:07.263');

            INSERT INTO Store
                VALUES (294, 'Store 294', 'demographics', 'guid', '2014-09-12 11:15:07.263');

            INSERT INTO Store
                VALUES (295, 'Store 295', 'demographics', 'guid', '2014-09-12 11:15:07.263');

            INSERT INTO Store
                VALUES (296, 'Store 296', 'demographics', 'guid', '2014-09-12 11:15:07.263');
            """
        ),
    )

    getml.database.execute("\n".join(queries), conn=conn)

    yield conn

    for table in getml.database.get("SHOW TABLES;", conn=conn).name:
        getml.database.execute(f"DROP TABLE {table};", conn=conn)
