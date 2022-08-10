.. _csv_interface:


CSV interface
-------------

The fastest way to import data into the getML engine is to read it
directly from CSV files.

.. _csv_interface_import:

Import from CSV
"""""""""""""""

Using the :func:`~getml.data.DataFrame.from_csv` class method, you can
create a new :class:`~getml.data.DataFrame` based on a table stored in
the provided file(s). The :meth:`~getml.data.DataFrame.read_csv`
method will replace the content of the current
:class:`~getml.data.DataFrame` instance or append further rows.

.. _csv_interface_export:

Export to CSV
"""""""""""""

In addition to reading data from a CSV file, you can also write an
existing :class:`~getml.data.DataFrame` back into one using
:meth:`~getml.data.DataFrame.to_csv`.
