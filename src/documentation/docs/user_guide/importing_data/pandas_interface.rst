.. _pandas_interface:

Pandas interface
----------------

Pandas [1]_ is one of the key packages used in most data science
projects done in Python. The associated import interface is one of the
slowest, but you can harness the good data exploration and manipulation
capabilities of this Python package.

.. _pandas_interface_import:

Import from Pandas
""""""""""""""""""

Using the :func:`~getml.data.DataFrame.from_pandas` class method, you
can create a new :class:`~getml.data.DataFrame` based on the provided
:py:class:`pandas.DataFrame`. The
:meth:`~.getml.data.DataFrame.read_pandas` method will replace the
content of the current :class:`~getml.data.DataFrame` instance or
append further rows.

.. _pandas_interface_export:

Export to Pandas
""""""""""""""""

In addition to reading data from a :py:class:`pandas.DataFrame`, you
can also write an existing :class:`getml.data.DataFrame` back into a
:py:class:`pandas.DataFrame` using
:meth:`~getml.data.DataFrame.to_pandas`. Due to the way data is stored
within the getML engine, the dtypes of the original
:py:class:`pandas.DataFrame` can not be restored properly and their
might be inconsistencies in the order of microseconds being introduced
into timestamps.

.. [1] `https://pandas.pydata.org/ <https://pandas.pydata.org/>`_
