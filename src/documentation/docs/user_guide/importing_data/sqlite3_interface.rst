.. _sqlite3_interface:


SQLite3 interface
-----------------
  
SQLite3 [1]_ is a popular in-memory database. It is faster than
classical relational databases like PostgreSQL, but less stable under massive
parallel access, and requires all contained data
sets be loaded into memory, which might fill up too much of your
RAM, especially for large data sets.

As with all other databases in the unified import interface of the
getML Python API, you have to first connect to it - using
:func:`~.getml.database.connect_sqlite3`.

.. _sqlite3_interface_import:
  
Import from SQLite3
"""""""""""""""""""

By selecting an existing table from your database in the
:func:`~getml.data.DataFrame.from_db` class method, you can create a
new :class:`~getml.data.DataFrame` containing all its data.
Alternatively you can use the :meth:`~.getml.data.DataFrame.read_db`
and :meth:`~.getml.data.DataFrame.read_query` methods to replace the
content of the current :class:`~getml.data.DataFrame` instance or
append further rows based on either a table or a specific query.

.. _sqlite3_interface_export:

Export to SQLite3
"""""""""""""""""

You can also write your results back into the SQLite3 database. By
providing a name for the destination table in
:meth:`getml.pipeline.Pipeline.transform`, the features generated
from your raw data will be written back. Passing it into
:meth:`getml.pipeline.Pipeline.predict` generates predictions
of the target variables to new, unseen data and stores the result into
the corresponding table.

.. [1] `https://sqlite.org/index.html <https://sqlite.org/index.html>`_
