.. |icon_about| image:: /res/ic_info_24px.svg

.. _the_getml_monitor:

The getML monitor
=================

The getML monitor contains information on 
the data imported into the engine as well as the
trained pipelines and their performance. It written in Go
and compiled into a binary that is separate from the getML engine.

.. _the_getml_monitor_accessing:

.. image:: /res/screenshot_login.png

The monitor is always started on the same machine as the engine.
The engine and the monitor use sockets to communicate. The monitor 
opens a HTTP port - 1709 per default - for you to
access it via your favorite internet browser. Entering the following
address into the navigation bar will point your browser to the monitor:

	http://localhost:1709

The HTTP port can only be accessed from within the host the getML
suite is running on. 

The main purpose of the monitor is to help you with your data science
project by providing visual feedback.
