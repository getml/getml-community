# Copyright 2025 Code17 GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

"""This module is a collection of utility functions for the overall
communication and the session management of the getML Engine.

In order to log into the Engine, you have to open your favorite
internet browser and enter [http://localhost:1709](http://localhost:1709) in the navigation bar. This tells it to
connect to a local TCP socket at port 1709 opened by the getML
Monitor. This will only be possible from within the same device!

??? example
    First of all, you need to start the getML Engine.
    Next, you need to create a new project or
    load an existing one.

    ```python
    getml.engine.list_projects()
    getml.engine.set_project('test')
    ```
    After doing all calculations for today you can shut down the getML
    Engine.

    ```python
    print(getml.engine.is_alive())
    getml.engine.shutdown()
    ```

Note:
    The Python process and the getML Engine must be located on
    the same machine. If you
    intend to run the Engine on a remote host, make sure to start your
    Python session on that device as well. Also, when using SSH sessions,
    make sure to start Python using `python &` followed by `disown` or
    using `nohup python`. This ensures the Python process and all the
    script it has to run won't be killed the moment your remote
    connection becomes unstable, and
    you are able to recover them later on (see
    [`remote_access`][remote-access-anchor]).

    All data frame objects and models in the getML Engine are
    bundled in projects. When loading an existing project, the
    current memory of the Engine will be flushed and all changes
    applied to [`DataFrame`][getml.DataFrame] instances after
    calling their [`save`][getml.DataFrame.save] method will
    be lost. Afterwards, all
    [`Pipeline`][getml.Pipeline] will be loaded into
    memory automatically. The data frame objects
    will not be loaded automatically since they consume significantly
    more
    memory than the pipelines. They can be loaded manually using
    [`load_data_frame`][getml.data.load_data_frame] or
    [`load`][getml.DataFrame.load].

    The getML Engine reflects the separation of data into individual
    projects on the level of the filesystem too. All data
    belonging to a single project is stored in a dedicated folder in
    the 'projects' directory located in '.getML' in your home
    folder. These projects can be copied and shared between different
    platforms and architectures without any loss of information. However,
    you must copy the entire project and not just individual data frames
    or pipelines.
"""

from getml.communication import is_engine_alive, is_monitor_alive

from ._launch import EXECUTABLE_NAME, Edition, launch
from .helpers import (
    delete_project,
    is_alive,
    list_projects,
    list_running_projects,
    set_project,
    shutdown,
    suspend_project,
)

__all__ = (
    "delete_project",
    "EXECUTABLE_NAME",
    "Edition",
    "is_alive",
    "is_engine_alive",
    "is_monitor_alive",
    "launch",
    "list_projects",
    "list_running_projects",
    "set_project",
    "shutdown",
    "suspend_project",
)
