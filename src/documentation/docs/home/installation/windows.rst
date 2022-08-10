Install getML on Windows/Docker 
===============================

getML for Docker is the recommended way of running getML on Windows. However, it
can also be used on macOS and Linux for a more "out-of-the-box" experience.

This installation guide explains all necessary steps to install getML on Windows/Docker.
To download the getML suite, go to 

https://getml.com/product/

and click the download button. This will download a ZIP archive containing
everything you need to use getML for Docker.

System requirements
^^^^^^^^^^^^^^^^^^^

Your system should meet the following requirements to
successfully install getML for Docker:

* Docker (`<https://www.docker.com/>`_). If you are on Linux, 
  make sure that you can run docker without root rights/sudo.

* Bash. This is pre-installed on Linux and macOS. For Windows
  users, we recommend Git Bash (`<https://gitforwindows.org/>`_).

* OpenSSL (`<https://www.openssl.org/>`_). This should be 
  pre-installed on most systems as well.

Setup and run getML for Docker 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1. Make sure that Docker is running (more precisely, the Docker daemon).

2. Unzip `getml-X.X.X-docker.zip`, 
   where `X.X.X` is a placeholder for 
   the version number.

3. Execute `setup.sh`. This will run the Dockerfile and
   setup your Docker image. It will also create a Docker
   volume called 'getml'. 
On Windows, you can just click on `setup.sh`.

On macOS and Linux, do the following:

.. code-block:: bash

    cd getml-X.X.X-docker
    bash setup.sh # or ./setup.sh

(Please make sure that you actually `cd` into that
directory, otherwise `setup.sh` will not find the
Dockerfile.)

4. Execute `run.sh`. This will run the Docker image. 

On Windows, you can just click on `run.sh`.

On macOS and Linux, do the following:

.. code-block:: bash

    cd getml-X.X.X-docker
    bash run.sh # or ./run.sh

Uninstall getML
^^^^^^^^^^^^^^^

To uninstall getML for Docker, execute 
`uninstall.sh`.

On Windows, you can just click on `uninstall.sh`.

On macOS and Linux, do the following:

.. code-block:: bash

    cd getml-X.X.X-docker
    bash uninstall.sh # or ./uninstall.sh

Where to go next
^^^^^^^^^^^^^^^^

The :ref:`Getting started guide <getting_started>` provides an
overview of the functionality of getML and a basic
example of how to use the Python API. In order to get help or provide feedback,
please contact our :ref:`support <support>`.
