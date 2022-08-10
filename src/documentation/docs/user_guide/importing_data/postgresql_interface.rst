.. _postgresql_interface:

PostgreSQL interface
--------------------

PostgreSQL [1]_ is a powerful and well-established open source
database system. It can be connected to the getML engine using the
function :func:`~.getml.database.connect_postgres`. But first, make
sure your database is running, you have the corresponding hostname,
port as well as your user name and password ready, and you can reach
it from via your command line.

.. _postgresql_interface_import:
  
Import from PostgreSQL
""""""""""""""""""""""

By selecting an existing table from your database in the
:func:`~getml.data.DataFrame.from_db` classmethod, you can create a
new :class:`~getml.data.DataFrame` containing all its data.
Alternatively you can use the :meth:`~.getml.data.DataFrame.read_db`
and :meth:`~.getml.data.DataFrame.read_query` methods to replace the
content of the current :class:`~getml.data.DataFrame` instance or
append further rows based on either a table or a specific query.

.. _postgresql_interface_export:

Export to PostgreSQL
""""""""""""""""""""

You can also write your results back into the PostgreSQL database. If you 
provide a name for the destination table in
:meth:`getml.pipeline.Pipeline.transform`, the features generated
from your raw data will be written back. Passing it into
:meth:`getml.pipeline.Pipeline.predict` generates predictions
of the target variables to new, unseen data and stores the result into
the corresponding table.

.. [1] `https://www.postgresql.org/ <https://www.postgresql.org/>`_
