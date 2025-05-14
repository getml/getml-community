# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

import pytest

import getml


@pytest.mark.parametrize(
    ("table_name", "columns", "expected_roles"),
    [
        (
            "Product",
            ["ProductID", "MakeFlag", "FinishedGoodsFlag", "Name", "ProductNumber"],
            [
                "unused_float",
                "unused_float",
                "unused_float",
                "unused_string",
                "unused_string",
            ],
        ),
        (
            "SalesOrderDetail",
            [
                "SalesOrderID",
                "SalesOrderDetailID",
                "OrderQty",
                "ProductID",
                "UnitPrice",
                "UnitPriceDiscount",
            ],
            [
                "unused_float",
                "unused_float",
                "unused_float",
                "unused_float",
                "unused_string",
                "unused_string",
            ],
        ),
        (
            "Store",
            ["BusinessEntityID", "Name", "Demographics", "rowguid", "ModifiedDate"],
            [
                "unused_float",
                "unused_string",
                "unused_string",
                "unused_string",
                "unused_string",
            ],
        ),
    ],
)
def test_from_query(
    getml_project, prepopulated_duckdb_conn, table_name, columns, expected_roles
):
    conn = prepopulated_duckdb_conn
    df = getml.DataFrame.from_query(
        name=f"{table_name}",
        query=f"SELECT {', '.join(columns)} FROM {table_name}",
        conn=conn,
    )
    expected_length = getml.database.get(
        f"SELECT COUNT(*) FROM {table_name}", conn=conn
    ).iloc[0, 0]
    assert df.name == table_name
    assert len(df) == expected_length
    assert len(df.columns) == len(columns)
    assert set(df.columns) == set(columns)
    assert [df[col].role for col in columns] == expected_roles
