# Copyright 2022 The SQLNet Company GmbH
# 
# This file is licensed under the Elastic License 2.0 (ELv2). 
# Refer to the LICENSE.txt file in the root of the repository 
# for details.
# 

"""
This is an integration test based on
the Adventure Works dataset.
"""


import pandas as pd  # type: ignore
import numpy as np

import getml as getml


def test_adventure_works():
    """
    This is an integration test based on
    the Adventure Works dataset.
    """

    getml.engine.launch()
    getml.set_project("adventure_works")

    conn = getml.database.connect_mariadb(
        host="relational.fit.cvut.cz",
        dbname="AdventureWorks2014",
        port=3306,
        user="guest",
        password="relational",
    )

    container = _make_container(conn)

    dm = _make_data_model(container)

    trimmer = getml.preprocessors.CategoryTrimmer()

    fast_prop = (
        getml.feature_learning.FastProp(  # pylint: disable=unexpected-keyword-arg
            loss_function=getml.feature_learning.loss_functions.CrossEntropyLoss,
            num_threads=1,
            num_features=400,
        )
    )

    predictor = getml.predictors.XGBoostClassifier(n_jobs=1)

    pipe1 = getml.Pipeline(
        tags=["fast_prop"],
        data_model=dm,
        preprocessors=[trimmer],
        feature_learners=[fast_prop],
        predictors=[predictor],
        include_categorical=True,
    )

    pipe1.check(container.train)
    pipe1.fit(container.train)
    scores = pipe1.score(container.test)

    assert scores.auc > 0.969, "Expected an AUC greater 0.969, got " + str(scores.auc)


def _load_if_needed(name: str, conn: getml.database.Connection):
    """
    Loads the data from the relational learning
    repository, if the data frame has not already
    been loaded.
    """
    if not getml.data.exists(name):
        data_frame = getml.DataFrame.from_db(name=name, table_name=name, conn=conn)
        data_frame.save()
    else:
        data_frame = getml.data.load_data_frame(name)
    return data_frame


def _make_product(conn: getml.database.Connection) -> getml.DataFrame:
    product = _load_if_needed("Product", conn)
    product.set_role("ProductID", getml.data.roles.join_key)
    product.set_role(
        ["MakeFlag", "ProductSubcategoryID", "ProductModelID"],
        getml.data.roles.categorical,
    )
    product.set_role(
        ["SafetyStockLevel", "ReorderPoint", "StandardCost", "ListPrice"],
        getml.data.roles.numerical,
    )
    return product


def _make_sales_order_detail(conn: getml.database.Connection) -> getml.DataFrame:
    sales_order_detail = _load_if_needed("SalesOrderDetail", conn)
    sales_order_detail.set_role(
        ["SalesOrderID", "SalesOrderDetailID", "ProductID", "SpecialOfferID"],
        getml.data.roles.join_key,
    )
    sales_order_detail.set_role(
        ["OrderQty", "UnitPrice", "UnitPriceDiscount", "LineTotal"],
        getml.data.roles.numerical,
    )
    sales_order_detail.set_role("ModifiedDate", getml.data.roles.time_stamp)
    return sales_order_detail


def _make_sales_order_header(conn: getml.database.Connection) -> getml.DataFrame:
    sales_order_header = _load_if_needed("SalesOrderHeader", conn)

    sales_order_header["SalesPersonIDCat"] = sales_order_header["SalesPersonID"]
    sales_order_header["TerritoryIDCat"] = sales_order_header["TerritoryID"]

    sales_order_header.set_role(
        ["CustomerID", "SalesOrderID", "SalesPersonID", "TerritoryID"],
        getml.data.roles.join_key,
    )
    sales_order_header.set_role(
        [
            "RevisionNumber",
            "OnlineOrderFlag",
            "SalesPersonIDCat",
            "TerritoryIDCat",
            "ShipMethodID",
        ],
        getml.data.roles.categorical,
    )
    sales_order_header.set_role(
        ["SubTotal", "TaxAmt", "Freight", "TotalDue"], getml.data.roles.numerical
    )
    sales_order_header.set_role(
        ["OrderDate", "DueDate", "ShipDate", "ModifiedDate"],
        getml.data.roles.time_stamp,
    )

    sales_order_header["churn"] = _calculate_churn(sales_order_header)

    sales_order_header = sales_order_header[~sales_order_header.churn.is_nan()].to_df(
        "SalesOrderHeaderRefined"
    )

    sales_order_header.set_role("churn", getml.data.roles.target)

    return sales_order_header


def _calculate_churn(sales_order_header: getml.DataFrame) -> np.ndarray:
    sales_order_header_pd = sales_order_header[
        ["OrderDate", "CustomerID", "SalesOrderID"]
    ].to_pandas()
    repeat_purchases = sales_order_header_pd.merge(
        sales_order_header_pd[["OrderDate", "CustomerID"]],
        on="CustomerID",
        how="left",
    )
    repeat_purchases = repeat_purchases[
        repeat_purchases["OrderDate_y"] > repeat_purchases["OrderDate_x"]
    ]
    repeat_purchases = repeat_purchases[
        repeat_purchases["OrderDate_y"] - repeat_purchases["OrderDate_x"]
        > pd.Timedelta("180 days")
    ]
    repeat_purchases = repeat_purchases.groupby(
        "SalesOrderID", as_index=False
    ).aggregate({"CustomerID": "max"})
    repeat_purchase_ids = {sid: True for sid in repeat_purchases["SalesOrderID"]}
    cut_off_date = max(sales_order_header_pd["OrderDate"]) - pd.Timedelta("180 days")
    return np.asarray(
        [
            np.nan
            if order_date >= cut_off_date
            else 0
            if order_id in repeat_purchase_ids
            else 1
            for (order_date, order_id) in zip(
                sales_order_header_pd["OrderDate"],
                sales_order_header_pd["SalesOrderID"],
            )
        ]
    )


def _make_sales_order_reason(conn: getml.database.Connection) -> getml.DataFrame:
    sales_order_reason = _load_if_needed("SalesOrderHeaderSalesReason", conn)
    sales_order_reason.set_role("SalesOrderID", getml.data.roles.join_key)
    sales_order_reason.set_role("SalesReasonID", getml.data.roles.categorical)
    return sales_order_reason


def _make_special_offer(conn: getml.database.Connection) -> getml.DataFrame:
    special_offer = _load_if_needed("SpecialOffer", conn)
    special_offer.set_role(["SpecialOfferID"], getml.data.roles.join_key)
    special_offer.set_role(["MinQty", "DiscountPct"], getml.data.roles.numerical)
    special_offer.set_role(
        ["Category", "Description", "Type"], getml.data.roles.categorical
    )
    special_offer.set_role(["StartDate", "EndDate"], getml.data.roles.time_stamp)
    return special_offer


def _make_store(conn: getml.database.Connection) -> getml.DataFrame:
    store = _load_if_needed("Store", conn)
    store.set_role(["SalesPersonID"], getml.data.roles.join_key)
    store.set_role(["SalesPersonID"], getml.data.roles.join_key)
    return store


def _make_container(conn: getml.database.Connection) -> getml.data.Container:
    product = _make_product(conn)
    sales_order_detail = _make_sales_order_detail(conn)
    sales_order_header = _make_sales_order_header(conn)
    sales_order_reason = _make_sales_order_reason(conn)
    special_offer = _make_special_offer(conn)
    store = _make_store(conn)

    split = getml.data.split.random(train=0.8, test=0.2)

    container = getml.data.Container(population=sales_order_header, split=split)
    container.add(
        product=product,
        sales_order_detail=sales_order_detail,
        sales_order_header=sales_order_header,
        sales_order_reason=sales_order_reason,
        special_offer=special_offer,
        store=store,
    )

    return container


def _make_data_model(container: getml.data.Container) -> getml.data.DataModel:
    dm = getml.data.DataModel(container.sales_order_header.to_placeholder("population"))

    dm.add(
        getml.data.to_placeholder(
            product=container.product,
            sales_order_detail=container.sales_order_detail,
            sales_order_header=container.sales_order_header,
            sales_order_reason=container.sales_order_reason,
            special_offer=container.special_offer,
            store=container.store,
        )
    )

    dm.population.join(
        dm.sales_order_header,
        on="CustomerID",
        time_stamps="OrderDate",
        lagged_targets=True,
        horizon=getml.data.time.days(1),
    )

    dm.population.join(
        dm.sales_order_detail,
        on="SalesOrderID",
    )

    dm.population.join(
        dm.sales_order_reason,
        on="SalesOrderID",
    )

    dm.population.join(
        dm.store,
        on="SalesPersonID",
    )

    dm.sales_order_detail.join(
        dm.product,
        on="ProductID",
        relationship=getml.data.relationship.many_to_one,
    )

    dm.sales_order_detail.join(
        dm.special_offer,
        on="SpecialOfferID",
        relationship=getml.data.relationship.many_to_one,
    )

    return dm
