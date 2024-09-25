## Table of Contents

* [Overview](#overview)
* [Installation](#installation)
  * [Python API](#python-api)
  * [Engine](#engine)
    * [Linux](#engine-linux)
    * [Docker (macOS, Windows, Linux)](#engine-docker)
* [Deinstallation](#deinstallation)
  * [Python API](#deinstallation-python-api)
  * [Linux](#deinstallation-linux)
* [Compiling from Source](#compiling-from-source)

## Overview <a name="overview"></a>

There are three components of the getML Community edition:

1. The Python API | Helps you interact with the getML Engine
2. The getML Engine | The C++ backend of getML
3. The CLI | A command line interface to install and interact with the Engine

On Linux, the getML Community edition binary of the Engine is shipped with the Python API.
On macOS and Windows, you will have to run the Engine in a Docker container.
We are working on providing native support for macOS and Windows in the near future.
The Python API is available for all platforms that support Python 3.8 or above.

## Installation <a name="installation"></a>

### Python API <a name="python-api"></a>

The Python API can be installed from the Python Package Index by executing the following command in a terminal:

```bash
pip install getml
```

Alternatively, if you want to install the API from the source,
you will have to install the Engine separately unless you install the wheel file provided by following the steps in the [Compiling from Source](#compiling-from-source) section.

```bash
pip install -e ./src/python-api # path to getml python-api
```

### Engine <a name="engine"></a>

Even though the Community edition of the Engine is shipped with the Python API on Linux platforms,
in some cases it might be preferred to install it separately. For example, if you want to use the [Enterprise edition](https://.....) of the Engine.

#### Linux <a name="engine-linux"></a>

Please execute the following commands, replacing `ARCH` with either `x64` or `arm64`, depending on your architecture.
If you are unsure, `x64` is probably the right choice.
You can also use `uname -m` to figure out the architecture.
If it says something like `aarch64` or `arm64`, you need to use [`arm64`](https://static.getml.com/download/1.5.0/getml-1.5.0-arm64-community-edition-linux.tar.gz), otherwise go with [`x64`](https://static.getml.com/download/1.5.0/getml-1.5.0-x64-community-edition-linux.tar.gz).

```bash
# Download the tar file of the Engine
wget https://static.getml.com/download/1.5.0/getml-community-1.5.0-<arch>-linux.tar.gz

# Extract the tar file
tar -xzf getml-community-1.5.0-<arch>-linux.tar.gz

# Change directory 
cd getml-community-1.5.0-<arch>-linux

# Install the Engine
./getML install
```

The output of the `install` command will tell you where the Engine has been installed.
It will look something like this:

```bash
getml@laptop src % ./getML install        
Installing getML...
Could not install into '/usr/local': mkdir /usr/local/getML: permission denied
Global installation failed, most likely due to missing root rights. Trying local installation instead.
Installing getML...
Successfully installed getML into '/Users/getml/.getML/getml-community-1.5.0-arm64-linux'.
Installation successful. To be able to call 'getML' from anywhere, add the following path to PATH:
/home/getml/.getML/getml-community-1.5.0-arm64-linux
```

To run the Engine, execute:

```bash
./getML
```

If the Engine was installed to the user home directory, you can add the installation directory to your PATH if you want to call the getML CLI from anywhere.

```bash
export PATH=$PATH:/path/to/getml-community-1.5.0-<arch>-linux
```

To make the changes permanent, you will have to add the line to your `.bashrc` or `.bash_profile` file.

#### Docker (macOS, Windows, Linux) <a name="engine-docker"></a>

Before the installation, make sure your system has [Docker](https://www.docker.com/) installed and running. On Linux, make sure that you follow these post-installation steps to run Docker without root rights/sudo: <https://docs.docker.com/engine/install/linux-postinstall/>

We offer two ways to start the Docker container. One is to use the provided docker-compose file and the other is to use the `docker run` command.

The docker-compose file is the recommended way to start the container and will be used in the following as it will automatically mount the necessary volumes, map the ports on the host system, and start the container.

First, download the docker-compose file in your project directory by executing the following command in a terminal:

```bash
curl -LO https://raw.githubusercontent.com/getml/getml-community/1.5.0/runtime/docker-compose.yml
```

By executing the following command, the getML service will be started. Further, a local directory `getml` will be created if it doesn't exist yet and mounted into the container. It will contain the files of the project. Also, the ports required for the Python API to communicate with the Engine will be mapped to the host system.

```bash
docker compose up
```

The docker compose file can also be piped into the docker compose command directly.

```bash
curl -L https://raw.githubusercontent.com/getml/getml-community/1.5.0/runtime/docker-compose.yml | docker-compose up -f -
```

To shut down the service after you are done, press `Ctrl+c`.

## Deinstallation <a name="deinstallation"></a>

### Python API <a name="deinstallation-python-api"></a>

> :warning: **Project data might be deleted**: If you have not installed the Engine separately and have not set the home directory to a custom location on Engine launch, the project data will be deleted when you uninstall the Python API.

To uninstall the Python API, execute the following command in a terminal:

```bash
pip uninstall getml
```

### Linux <a name="deinstallation-linux"></a>

> :warning: **Project data might be deleted**: If you have not set the home directory to a custom location on Engine launch, the project data will be deleted when you remove the `.getML` directory.

You will have to remove the folder `.getML` from your home directory. To do so, execute the following command in a terminal:

```bash
rm -r $HOME/.getML
```

## Compiling from Source <a name="compiling-from-source"></a>

Refer to [BUILD.md](BUILD.md) for instructions on how to compile from source and build the getML Suite.
