.. |icon_shutdown| image:: /res/ic_power_settings_new_24px.svg
.. |icon_log| image:: /res/ic_code_24px.svg
.. |icon_getml| image:: /res/icon_24x24.png
						   
.. _the_getml_engine:


The getML engine
================

The getML engine is a standalone program written in C++ that does the
actual work of feature engineering and prediction. 

.. _the_getml_engine_starting:

Starting the engine
^^^^^^^^^^^^^^^^^^^^^^

The engine can be started using the dedicated launcher icon or by
using the getML command line interface (CLI). For more information
check out the :ref:`installation instructions <installation>` for your
operating system.

.. _the_getml_engine_stopping:

Shutting down the engine
^^^^^^^^^^^^^^^^^^^^^^^^

There are several ways to shut down the getML engine:

- Click the '|icon_shutdown| Shutdown' tab in the sidebar of the monitor
- Press :kbd:`Ctrl-C` (if started via the command line)
- Run the getML command-line interface (CLI) (see :ref:`installation`)
  using the :code:`-stop` option
- macOS only: Right-click the getML icon |icon_getml| in the status bar and click 'Quit' (if
  started via the launchpad)

.. _the_getml_engine_logging:

Logging
^^^^^^^

The engine keeps a log about what it is currently
doing. 

The easiest way to view the log is to click the '|icon_log| Log' tab in the
sidebar of the getML monitor. The engine will also output its log to the command
line when it is started using the command-line interface.

