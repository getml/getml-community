.. _api_documentation_cli:

Command Line Interface
=======================

getML can be launched via the command line. The command line
interface is called :code:`getML` on Linux, :code:`getml-cli`
on macOS and :code:`getML.exe` on Windows.

Refer to the :ref:`installation section<installation>`, for instructions for how to set up
the command line interface.

Some parameters can be set via command line flags. If you do not explicitly set them,
the values from your *config.json* are taken instead. The *config.json* is located 
in :code:`$HOME/.getML/getml-VERSION` on Linux and macOS. On Windows, it is located 
in the same directory as :code:`getML.exe`. The most elegant way to edit your *config.json*
is via the :ref:`configuration view<the_getml_monitor_configuration>` in the getML 
:ref:`monitor<the_getml_monitor>`:

The help menu can also be displayed by passing the flag :code:`-help` or :code:`-h`. The default
values displayed in the help menu are the values in the *config.json* (therefore, they are not
hard-coded).

.. code::

    usage: ./getML <command> [<args>] or ./getML [<args>].

.. code::

    Possible commands are:
     run        Runs getML. Type "./getML -h" or "./getML run -h" to display the arguments. "run" is executed by default.
     install    Installs getML.
     stop       Stops a running instance of getML. Type "./getML stop -h" to display the arguments.
     uninstall  Uninstalls getml-0.14-beta-macos.
     version    Prints the version (getml-0.14-beta-macos).
    
.. code::

    Usage of run:
      -allow-push-notifications
            Whether you want to allow the getML monitor to send push notifications to your desktop. (default true)
      -http-port int
            The local port of the getML monitor. This port can only be accessed from your local computer. (default 1709)
      -https-port int
            The remote and encrypted port of the getML monitor. This port can be accessed remotely, but it is encrypted. (default 1710)
      -install
            Installs getml-0.14-beta-macos, even if it is already installed.
      -launch-browser
            Whether you want to automatically launch your browser. (default true)
      -project-directory string
            The directory in which to store all of your projects. (default "~/.getML/getml-0.14-beta-macos/projects")
      -proxy-url string
            The URL of any proxy server that that redirects to the getML monitor.
      -tcp-port int
            Local TCP port which serves as the communication point for the engine. This port can only be accessed from your local computer. (default 1711)

.. code::

    Usage of stop:
      -tcp-port int
            The TCP port of the getML instance you would like to stop. (default 1711)

