.. _linux:


Install getML on Linux
======================

This installation guide explains all necessary steps to install getML on Linux.
To download the getML suite, go to 

https://getml.com/product/

and click the download button. This will download a tarball containing
everything you need to use getML: The :ref:`getML engine<the_getml_engine>`,
the :ref:`getML monitor<the_getml_monitor>`, and the :ref:`Python API
<the_getml_python_api>`.


System requirements
^^^^^^^^^^^^^^^^^^^

Your Linux should meet at least the following requirements to
successfully install getML

* **GLIBC** 2.28 or above (check by using :code:`ldd --version`)
  
* If you are using **Fedora 30**, you need *libxcrypt-compat*. Install
  using :code:`yum install libxcrypt-compat`.

* Python 3.7 or above must be installed on your machine. Furthermore, `numpy`
  and `pandas` are required dependencies for the getML Python API.

_____

Install and run the getML engine and monitor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The getML engine is the C++ backend of getML. It comes with a graphical
user interface - the getML monitor - that runs in your browser. To install these
components, do the following:

1. Extract the tarball using 
   :code:`tar -xzvf getml-VERSION-linux.tar.gz`.

2. :code:`cd` into the resulting folder. Then, type :code:`./getML`.
   This will create a hidden folder called :code:`.getML` in your
   home directory. If your computer has a desktop environment, you
   will also have a getML icon in your Applications menu. 
   
3. (optional) You can now install get getML command-line interface
   (getML CLI). See below for further instructions. 
     
4. Open a browser and visit http://localhost:1709/ (if launching
   getML did not point you there automatically). 


Install the getML Python API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The Python API is a convenient way to interact with and to control the
getML engine. There are two options to install the getML Python API

From PyPI 
------------

In a terminal, execute the following command to install the remote
version from the Python Package Index

.. code-block:: bash

    pip install getml
    
To make sure that the Python API was installed properly, you can use

.. code-block:: bash

    python -c 'import getml'

_____

Install the getML CLI
^^^^^^^^^^^^^^^^^^^^^

getML comes with a :ref:`command-line interface<api_documentation_cli>` (CLI) 
that lets you configure the most
important parameters on startup. The CLI is a standalone Go-binary located in
the downloaded bundle.

.. note::

   Before you can use the CLI, you have to follow steps 1 and 2 of the installation
   instructions above.

After deflating the tarball, you should find the :code:`getML` binary in the
resulting folder.

After you have started getML for the first time, 
you can move the :code:`getML` binary anywhere you want. 
We recommend moving the :code:`getML` binary
to a location included in the :envvar:`PATH` environment variable,
such as :code:`~/.local/bin`. You can inspect the content of the
aforementioned variable in a shell using

.. code-block:: bash
				
   echo $PATH

and check if it can be properly found by executing

.. code-block:: bash
				
	which getML
	
If this returns the location you moved the binary to, you are
ready to go.

For further help on how to use the CLI, just use :code:`getML -h` or :code:`getML -help`: 

Uninstall getML
^^^^^^^^^^^^^^^

To uninstall getML from your computer

1. Remove the folder :code:`.getML` from your home directory. To do
   so, open a terminal and enter the following command
   
.. code-block::

   rm -r $HOME/.getML
   
3. Delete :code:`getML` binary from wherever you have put it (if you have
   decided to install the getML CLI).


Where to go next
^^^^^^^^^^^^^^^^

The :ref:`Getting started guide <getting_started>` provides an
overview of the functionality of getML and a basic
example of how to use the Python API. In order to get help or provide feedback,
please contact our :ref:`support <support>`.
