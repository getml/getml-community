.. _mariadb_interface:

MariaDB interface
-----------------------

MariaDB [1]_ is a popular open source fork of MySQL. It can be
connected to the getML engine using the function
:func:`~.getml.database.connect_mariadb`. But first, make sure your
database is running, you have the corresponding hostname, port as well
as your user name and password ready, and you can reach it from via
your command line.

If you are unsure which port or socket your MariaDB is running
on, you can start the `mysql` command line interface 


.. code-block:: bash
	
   $ mysql

and use the following queries to get the required insights.

.. code-block:: sql

   MariaDB [(none)]> SELECT @@port;

   MariaDB [(none)]> SELECT @@socket;



.. _mariadb_interface_import:
  
Import from MariaDB
"""""""""""""""""""

By selecting an existing table of your database in the
:func:`~getml.data.DataFrame.from_db` class method, you can create a
new :class:`~getml.data.DataFrame` containing all its data.
Alternatively you can use the :meth:`~.getml.data.DataFrame.read_db`
and :meth:`~.getml.data.DataFrame.read_query` methods to replace the
content of the current :class:`~getml.data.DataFrame` instance or
append further rows based on either a table or a specific query.

.. _mariadb_interface_export:

Export to MariaDB
"""""""""""""""""

You can also write your results back into the MariaDB database. By
providing a name for the destination table in
:meth:`getml.pipeline.Pipeline.transform`, the features generated
from your raw data will be written back. Passing it into
:meth:`getml.pipeline.Pipeline.predict` generates predictions
of the target variables to new, unseen data and stores the result into
the corresponding table.

.. [1] `https://mariadb.org/ <https://mariadb.org/>`_
