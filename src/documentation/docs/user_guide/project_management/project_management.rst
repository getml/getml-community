.. |icon_projects| image:: /res/ic_folder_24px.svg

.. _project_management:

Managing projects
=================

When working with getML, all data is bundled into *projects.* getML's projects
are managed through the :mod:`getml.project` module.

The relationship between projects and engine processes
------------------------------------------------------

Each project is tied to a specific instance of the :ref:`getML engine
<the_getml_engine>` running as a global process (independent from your python
sesssion). In this way it is possible to share one getML instance with multiple
users to work on different projects. When switching projects (though
:func:`getml.project.switch`), the python API spawns a new process and
establishes a connection to this process, while the currently loaded project
remains in memory and its process is delegated to the background (until you
explicitly :func:`~getml.project.suspend` the project). You can also work on
multiple projects simultaneously from different python sessions. This comes in 
particularly handy, if you use `Jupyter Lab <https://jupyter.org/>`_ to open
multiple notebooks and manage multiple python kernels simultaneously. 

To load an existing project or create a new one, 
you can do so from the '|icon_projects| Projects' view in the monitor or use the
API (:func:`getml.engine.set_project`). 

If you want to shutdown the engine process associated with the current project,
you can call :func:`getml.project.suspend()`. When you suspend the project, the
memory of the engine is flushed and all unsaved changes to the data frames are
lost (see :ref:`the_getml_python_api_lifecycles` for details). All pipelines of the
new project are automatically loaded into memory. You can retrieve all of your
project's pipelines through :attr:`getml.project.pipelines`.

Projects can be deleted by clicking the trash can icon in the '|icon_projects|
Projects' tab of the getML monitor or by calling
:func:`getml.engine.delete_project` (to delete a project by name) or
:func:`getml.project.delete` (to suspend and delete the project currently
loaded).

.. _project_management_folder:

Managing data using projects
----------------------------

Every project has its own folder in
:code:`~/.getML/getml-VERSION/projects` (for Linux and macOS)
in which all of its data
and pipelines are stored. On Windows, the projects folder is in the same
location as :code:`getML.exe`. These folders can be easily 
shared between different instances of getML; even between different
operating systems. However, individual pipelines or data frames cannot
be simply copied to another project folder – they are tied to the
project. Projects can be bundled and exported/imported.

Using the project module to manage your project
-----------------------------------------------

The :mod:`getml.project` module is the entry point to your projects. From here,
you can: query project-specific data (:const:`~getml.project.pipelines`,
:const:`~getml.project.data_frames`, :const:`~getml.project.hyperopts`), manage
the state of the current project (:func:`~getml.project.delete`,
:func:`~getml.project.restart`, :func:`~getml.project.switch`,
:func:`~getml.project.suspend`), and import projects from or export projects as
a :file:`.getml` bundle to disk (:func:`~getml.project.load`,
:func:`~getml.project.save`).

