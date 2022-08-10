.. _the_getml_python_api:

The getML Python API
====================

The getML Python API is shipped along with the matching version of getML
engine and monitor in the file you can download from `getml.com
<https://getml.com>`_ (see :ref:`installation`).

The most important thing you have to keep in mind when working with
the Python API is this:

	**All classes in the Python API are just handles to objects living in the
	getML engine.**
	
In addition, two basic requirements need to be fulfilled to successfully
use the API:

1. You need a running getML engine (on the same host as your Python
   session) (see :ref:`the_getml_engine_starting`)
2. You need to set a project in the getML engine using 
   :func:`getml.engine.set_project`.
   
   .. code-block:: python
	   
	   import getml
	   getml.engine.set_project('test')

This section provides some general information about the API and how
it interacts with the engine. For an in-depth read about its
individual classes, check out the :ref:`Python API
documentation<python_api>`.


.. _the_getml_python_api_engine:

Connecting to the getML engine
------------------------------

The getML Python API automatically connects to the engine with
every command you execute. It establishes a socket connection to the
engine through a port inferred by the time you connect to a project (or create a
new project.)

.. _the_getml_python_api_session:

Session management
------------------

You can set the current project (see :ref:`project_management`)
using :func:`getml.engine.set_project`. If no project matches the 
supplied name, a new one will be created. To get a
list of all available projects in your engine, you can use
:func:`getml.engine.list_projects` and to remove an entire project,
you can use :func:`getml.engine.delete_project`.

.. _the_getml_python_api_lifecycles:

Lifecycles and synchronization between engine and API
-----------------------------------------------------

The most important objects are the following:

- Data frames (:class:`~getml.data.DataFrame`), which act as a container
  for all your data.
- Pipelines (:class:`~getml.pipeline.Pipeline`), which hold the trained states of 
  the algorithms.
  
.. _the_getml_python_api_lifecycles_dataframe:

Lifecycle of a :class:`~getml.data.DataFrame`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create a :class:`~getml.data.DataFrame` by calling one of
the :func:`~getml.data.DataFrame.from_csv`,
:func:`~getml.data.DataFrame.from_db`,
:func:`~getml.data.DataFrame.from_json`, or
:func:`~getml.data.DataFrame.from_pandas` classmethods. These create
a data frame object in the getML engine, import the provided data,
and return a handler to the object as a
:class:`~getml.data.DataFrame` in the Python API (see
:ref:`importing_data`).

When you apply any method, like :meth:`~getml.data.DataFrame.add`, the
changes will be automatically reflected in both the engine and Python. 
Under the hood, the Python API sends a
command to create a new column to
the getML engine. The moment the engine is done, it informs the Python
API and the latter triggers the :meth:`~getml.data.DataFrame.refresh`
method to update the Python handler.

Data frames are **never saved automatically** and **never loaded
automatically**. All unsaved changes to a  :class:`~getml.data.DataFrame` will
be  lost when restarting the engine. To save a :class:`~getml.data.DataFrame`,
use  :meth:`~getml.data.DataFrame.save`. You can also use batch operations
(:meth:`getml.project.data_frames.save_all()<getml.data.DataFrames.save_all>`,
:meth:`getml.project.data_frames.load_all()<getml.data.DataFrames.load_all>` )
to save or load all data frames associated with the current project. The
:class:`~getml.project.DataFrames` container for the current project in memory can
be accessed through :attr:`getml.project.data_frames`.

To access the container, holding all of a project's data frames:

.. code-block:: python
				
	getml.project.data_frames

To save all data frames in memory to the project folder:

.. code-block:: python
				
	getml.project.data_frames.save_all()
                
You can subset the container to access single :class:`~getml.data.DataFrame`
instances.  You can then call all available methods those instances. E.g. to
store a single data frame to disk:

.. code-block:: python
				
	getml.project.data_frames[0].save()

If a :class:`~getml.data.DataFrame` 
called NAME_OF_THE_DATA_FRAME is already available in memory,
:func:`getml.data.load_data_frame` will return a handle to that
data frame. If no such :class:`~getml.data.DataFrame` is held
in memory, the function will try to load the data frame from disk and 
then return a handle. If that is unsuccessful, an exception is
thrown.

If you want to force the API to load the 
version stored on disk over the one
held in memory, you can use the :meth:`~getml.data.DataFrame.load`
as follows:

.. code-block:: python
				
   df = getml.data.DataFrame(NAME_OF_THE_DATA_FRAME).load()

.. _the_getml_python_api_lifecycles_models:

Lifecycle of a :mod:`~getml.pipeline.Pipeline`
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The lifecycle of a
:mod:`getml.pipeline.Pipeline` is straightforward since the getML engine 
automatically 
saves all changes made to a pipeline and automatically loads all
pipelines contained in a project.

Using the constructors, the individual pipelines are created within the
Python API, where they are represented as a set of
hyperparameters. The actual weights of the machine learning algorithms
are only stored in the getML engine and never transferred to the
Python API. 

When applying any method, like
:meth:`~getml.pipelines.Pipeline.fit`, the changes will be
automatically reflected in both the engine and
the Python API. 

When using :func:`getml.engine.set_project` to load an existing
project, all pipelines contained in that project 
will be automatically loaded into memory. 
You can get an overview of all pipelines associated with the current project
by accessing the :class:`~getml.data.Pipelines` container, accessible through
:attr:`~getml.project_pipelines`.

In order to create a corresponding handle in the Python API, you can use
:func:`getml.pipeline.load`: 

.. code-block:: python
				
	pipe = getml.pipeline.load(NAME_OF_THE_PIPELINE)

The function :func:`getml.pipeline.list_pipelines` lists all
available pipeline within a project.
