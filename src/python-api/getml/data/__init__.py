# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""Contains functionalities for importing, handling, and retrieving
data from the getML Engine.

All data relevant for the getML Suite has to be present in the getML
Engine. Its Python API itself does not store any of the data used for
training or prediction. Instead, it provides a handler class for the
data frame objects in the getML Engine, the
[`DataFrame`][getml.DataFrame]. Either using this overall handler for
the underlying data set or the individual [`columns`][getml.data.columns]
it is composed of, one can both import and retrieve data from the Engine
as well as performing operations on them. In addition to the data
frame objects, the Engine also uses an abstract and lightweight
version of the underlying data model, which is represented by the
[`Placeholder`][getml.data.Placeholder].

In general, working with data within the getML Suite is organized in
three different steps.

* [Importing the data][importing-data] into the getML Engine .
* [Annotating the data][annotating-data] by assigning
  [`roles`][getml.data.roles] to the individual [`columns`][getml.data.columns]
* [Constructing the data model][data-model-concepts] by deriving
  [`Placeholder`][getml.data.Placeholder] from the data and joining them to
  represent the data schema.

??? example
    Creating a new data frame object in the getML Engine and importing
    data is done by one of the class methods
    [`from_csv`][getml.DataFrame.from_csv],
    [`from_db`][getml.DataFrame.from_db],
    [`from_json`][getml.DataFrame.from_json], or
    [`from_pandas`][getml.DataFrame.from_pandas].

    In this example we chose to directly load data from a public
    database in the internet. But, firstly, we have to connect the
    getML Engine to the database (see [MySQL interface][mysql] in the user
    guide for further details).


    ```python
    getml.database.connect_mysql(
        host="relational.fel.cvut.cz",
        dbname="financial",
        port=3306,
        user="guest",
        password="ctu-relational",
        time_formats=['%Y/%m/%d']
    )
    ```

    Using the established connection, we can tell the Engine to
    construct a new data frame object called `df_loan`, fill it with
    the data of `loan` table contained in the MySQL database, and
    return a [`DataFrame`][getml.DataFrame] handler associated with
    it.

    ```python

    loan = getml.DataFrame.from_db('loan', 'df_loan')

    print(loan)
    ```
    ```
    | loan_id      | account_id   | amount       | duration     | date          | payments      | status        |
    | unused float | unused float | unused float | unused float | unused string | unused string | unused string |
    -------------------------------------------------------------------------------------------------------------
    | 4959         | 2            | 80952        | 24           | 1994-01-05    | 3373.00       | A             |
    | 4961         | 19           | 30276        | 12           | 1996-04-29    | 2523.00       | B             |
    | 4962         | 25           | 30276        | 12           | 1997-12-08    | 2523.00       | A             |
    | 4967         | 37           | 318480       | 60           | 1998-10-14    | 5308.00       | D             |
    | 4968         | 38           | 110736       | 48           | 1998-04-19    | 2307.00       | C             |
    ```
    In order to construct the data model and for the feature
    learning algorithm to get the most out of your data, you have
    to assign roles to columns using the
    [`set_role`][getml.DataFrame.set_role] method (see
    [Annotating data][annotating-data] for details).

    (For demonstration purposes, we assign `payments` the [`target role`][getml.data.roles.target]. In reality, you would want to
    forecast the defaulting behaviour, which is encoded in the `status` column. See the [loans notebook](https://github.com/getml/getml-demo/blob/master/loans.ipynb).)

    ```python

    loan.set_role(["duration", "amount"], getml.data.roles.numerical)
    loan.set_role(["loan_id", "account_id"], getml.data.roles.join_key)
    loan.set_role("date", getml.data.roles.time_stamp)
    loan.set_role(["payments"], getml.data.roles.target)

    print(loan)
    ```
    ```
    | date                        | loan_id  | account_id | payments  | duration  | amount    | status        |
    | time stamp                  | join key | join key   | target    | numerical | numerical | unused string |
    -----------------------------------------------------------------------------------------------------------
    | 1994-01-05T00:00:00.000000Z | 4959     | 2          | 3373      | 24        | 80952     | A             |
    | 1996-04-29T00:00:00.000000Z | 4961     | 19         | 2523      | 12        | 30276     | B             |
    | 1997-12-08T00:00:00.000000Z | 4962     | 25         | 2523      | 12        | 30276     | A             |
    | 1998-10-14T00:00:00.000000Z | 4967     | 37         | 5308      | 60        | 318480    | D             |
    | 1998-04-19T00:00:00.000000Z | 4968     | 38         | 2307      | 48        | 110736    | C             |
    ```
    Finally, we are able to construct the data model by deriving
    [`Placeholder`][getml.data.Placeholder] from each
    [`DataFrame`][getml.DataFrame] and establishing relations between
    them using the [`join`][getml.data.Placeholder.join] method.

    ```python
    # But, first, we need a second data set to build a data model.
    trans = getml.DataFrame.from_db(
        'trans', 'df_trans',
        roles = {getml.data.roles.numerical: ["amount", "balance"],
                 getml.data.roles.categorical: ["type", "bank", "k_symbol",
                                                "account", "operation"],
                 getml.data.roles.join_key: ["account_id"],
                 getml.data.roles.time_stamp: ["date"]
        }
    )

    ph_loan = loan.to_placeholder()
    ph_trans = trans.to_placeholder()

    ph_loan.join(ph_trans, on="account_id",
                time_stamps="date")
    ```

    The data model contained in `ph_loan` can now be used to
    construct a [`Pipeline`][getml.Pipeline].
"""

from . import access, relationship, roles, split, subroles, time
from .columns import arange, random, rowid
from .concat import concat
from .container import Container
from .data_frame import DataFrame
from .data_model import DataModel
from .helpers import OnType, list_data_frames
from .helpers2 import (
    _decode_data_model,
    _decode_placeholder,
    _load_view,
    delete,
    exists,
    load_data_frame,
    make_target_columns,
    to_placeholder,
)
from .load_container import load_container
from .placeholder import Placeholder, TimeStampsType
from .roles.container import Roles
from .star_schema import StarSchema
from .subset import Subset
from .time_series import TimeSeries
from .view import View

__all__ = (
    "access",
    "Container",
    "DataFrame",
    "DataModel",
    "Placeholder",
    "Roles",
    "StarSchema",
    "Subset",
    "TimeSeries",
    "View",
    "arange",
    "concat",
    "delete",
    "exists",
    "load_container",
    "load_data_frame",
    "list_data_frames",
    "make_target_columns",
    "OnType",
    "TimeStampsType",
    "random",
    "relationship",
    "roles",
    "rowid",
    "split",
    "subroles",
    "time",
    "to_placeholder",
    "_decode_data_model",
    "_decode_placeholder",
    "_load_view",
)
