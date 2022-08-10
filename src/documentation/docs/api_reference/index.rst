.. _python_api:

.. API Documentation

Python API
=================

Welcome to the API documentation for Python. The Python API is a convenient,
easy to use interface to the :ref:`getML engine<the_getml_engine>`. General
information about the interoperation of the different parts of getML can be
found in the :ref:`user guide<the_getml_python_api>`.

If you have never user the Python API it is probably easiest to start with the
:ref:`getting started guide<getting_started>`.


.. toctree::
   :caption: Modules
   :maxdepth: 2

   data <dataframe>
   database <database>
   datasets <datasets>
   engine <engine>
   hyperopt <hyperopt>   
   feature_learning <feature_learning>
   project <project>
   preprocessors <preprocessors>
   pipeline <pipeline>
   predictors <predictors>
   sqlite3 <sqlite>

.. automodule:: getml

Classes
~~~~~~~~~

.. autosummary::
   :toctree: ../api/data/
   :template: autosummary_theme/class.rst    

   ~DataFrame

.. autosummary::
   :toctree: ../api/pipeline/
   :template: autosummary_theme/class.rst

   ~Pipeline

Functions
~~~~~~~~~

.. autosummary::
   :toctree: ../api/engine/
   :template: autosummary_theme/function.rst    

   ~set_project
