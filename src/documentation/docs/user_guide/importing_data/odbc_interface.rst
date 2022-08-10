.. _odbc_interface:

ODBC interface 
--------------------

ODBC [1]_ is short for Open Database Connectivity. ODBC is an API 
specification for connecting software programming 
language to a database. ODBC is developed by Microsoft.

In a nutshell it works like this:

Any database of relevance has an *ODBC driver*. The ODBC driver 
translates calls from the ODBC API into a format the database
can understand. It returns the results of the query in a format 
the ODBC API can understand.

If you want to connect getML or any other software to a database
using ODBC, you have to first install the ODBC driver provided by
your database vendor.

In theory, ODBC drivers should support translating queries from the
SQL 99 standard into the SQL dialect. In practice, this requirement is
often ignored. Also, not all ODBC drivers support all ODBC calls.

At getML, we try to use native APIs for connecting to relational 
databases whereever possible. Whenever we cannot do that due to licensing
or other restrictions, we use ODBC.

In particular, we use ODBC for connecting to proprietary databases like 
**Oracle**, **Microsoft SQL Server** or **IBM DB2**.

ODBC is pre-installed in modern Windows operating systems. For Linux and macOS,
two open-source implementations exist, namely unixODBC and iODBC. getML uses
unixODBC for Linux and macOS.

An example: Microsoft SQL Server
"""""""""""""""""""""""""""""""""

In this example, we will show you how to connect to Microsoft SQL Server using
ODBC. If you do not have an Microsoft SQL Server instance at your disposal, you can
actually `download <https://www.microsoft.com/en-us/sql-server/sql-server-downloads>`_ 
a trial or development version for free.

The first step is to download the 
`ODBC driver <https://docs.microsoft.com/en-us/sql/connect/odbc/microsoft-odbc-driver-for-sql-server>`_
for SQL Server.

The second step is to configure the ODBC driver. Many ODBC drivers provide customized scripts for this,
so you might not have to do it by hand.

.. (commented out until we have an explicit Windows version) On **Windows**, you can use the `ODBC Data Source Administrator <https://docs.microsoft.com/en-us/sql/odbc/admin/odbc-data-source-administrator>`_.  

On **Linux and macOS**, you create a file named *.odbc.ini* in your 
home directory (if no such file already exists). In that file, leave an entry like this:

::

    [ANY-NAME-YOU-WANT]
    Driver = /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.5.so.2.1
    Server = 123.45.67.890
    Port = 1433
    User = YOUR-USERNAME
    Password = YOUR-PASSWORD
    Database = YOUR-DATABASE
    Language = us_english
    NeedODBCTypesOnly = 1

On **Docker** you can make appropriate changes to the Dockerfile and then rerun `./setup.sh` or `bash setup.sh`. 

You will need to set the following parameters:

* The first line is the *server name* or *data source name*. You can
  use this name to tell getML that this is the server you want to
  connect to.

* The *Driver* is the location of the ODBC driver you have just downloaded.
  The location or file name might be different on your system.

* The *Server* is the IP address of the server. If the server is on the 
  same machine as getML, just write "localhost".

* The *Port* is the port on which to connect the server. The default port
  for SQL Server is 1433.

* *User* and *Password* are the user name and password that allow to access
  the server.

* The *Database* is the database inside the server you want to connect to.

You can now connect getML to the database:

::
    
    getml.database.connect_odbc(
        server_name="ANY-NAME-YOU-WANT",
        user="YOUR-USERNAME",
        password="YOUR-PASSWORD",
        escape_chars="[]")

Important: Always pass *escape_chars*
""""""""""""""""""""""""""""""""""""""""

Earlier we mentioned that ODBC drivers are *supposed* to translate standard SQL
queries into the specific SQL dialects, but that this requirement is often ignored.

A typical example of this issue are *escape characters*. Escape characters are needed
when the names of your schemas, tables or columns are SQL keywords. For instance, the 
loans dataset contains a table named *ORDER*, which is a registered SQL keyword.

To avoid this problem, you can envelop the schema, table and column names 
in *escape characters*, like this:

.. code-block:: sql

    SELECT "some_column" FROM "SOME_SCHEMA"."SOME_TABLE";

getML *always* uses escape characters for its automatically generated queries.

The SQL standard requires that the quotation mark (") be used as the escape character. 
However, many SQL
dialects to not follow this requirement. For instance, SQL Server uses "[]":

.. code-block:: sql

    SELECT [some_column] FROM [SOME_SCHEMA].[SOME_TABLE];

MySQL and MariaDB work like this:

.. code-block:: sql

    SELECT `some_column` FROM `SOME_SCHEMA`.`SOME_TABLE`;

To save yourself some frustration, please figure your server's escape characters and then
explicitly pass them to :func:`~getml.database.connect_odbc`.


.. _odbc_interface_import:
  
Import data using ODBC 
"""""""""""""""""""""""

By selecting an existing table from your database in the
:func:`~getml.data.DataFrame.from_db` class method, you can create a
new :class:`~getml.data.DataFrame` containing all its data.
Alternatively you can use the :meth:`~.getml.data.DataFrame.read_db`
and :meth:`~.getml.data.DataFrame.read_query` methods to replace the
content of the current :class:`~getml.data.DataFrame` instance or
append further rows based on either a table or a specific query.

.. _odbc_interface_export:

Export data using ODBC 
"""""""""""""""""""""""

You can also write your results back into the PostgreSQL database. When you 
provide a name for the destination table in
:meth:`getml.pipeline.Pipeline.transform`, the features generated
from your raw data will be written back. Passing it into
:meth:`getml.pipeline.Pipeline.predict` generates predictions
of the target variables to new, unseen data and stores the result into
the corresponding table.

.. [1] `https://docs.microsoft.com/en-us/sql/odbc/reference/what-is-odbc <https://docs.microsoft.com/en-us/sql/odbc/reference/what-is-odbc>`_
