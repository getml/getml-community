.. _json_interface:


JSON interface
--------------

The another convenient but slow way to import data into the getML
engine via its Python API.

.. _json_interface_import:

Import from JSON
""""""""""""""""

Using the :func:`~getml.data.DataFrame.from_json` class method, you
can create a new :class:`~getml.data.DataFrame` based on a JSON
string. The :meth:`~getml.data.DataFrame.read_json` method will
replace the content of the current :class:`~getml.data.DataFrame`
instance or append further rows.

.. _json_interface_export:

Export to JSON
""""""""""""""

In addition to reading data from a JSON string, you can also convert an
existing :class:`~getml.data.DataFrame` into one using
:meth:`~getml.data.DataFrame.to_json`.
