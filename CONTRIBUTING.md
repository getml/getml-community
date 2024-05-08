# Contributing

## Table of Contents

* [Installation](#installation)
  * [Linux](#linux)
  * [Docker](#docker-for-macos-windows-or-linux)
* [Deinstallation](#deinstallation)
  * [Linux](#linux-1)
  * [Docker](#docker)
* [Compiling from source](#compiling-from-source)

## Installation

### Linux

Before the installation, make sure your Linux meets the following requirements:
- GLIBC 2.17 or above (check by using `ldd --version`)
- On Fedora 30, you need _libxcrypt-compat_. Install using: `yum install libxcrypt-compat`.
- Python 3.8 or above, _numpy_ and _pandas_ (for the Python API).


There are two components of the getML community edition: 
1. The Python API | Helps you interact with the getML engine
2. The getML engine | The C++ backend of getML

The Python API can be installed from the Python Package Index by executing the following command in a terminal:

```bash
pip install getml
```

The getML engine comes with the Python API and is installed automatically. However, if you want to install it separately, please execute the following commands,
replacing `ARCH` with either `x64` or `arm64`, depending on your architecture. If you are unsure, `x64` is probably the right choice. You can also use
`uname -m` to figure out the architecture. If it says something like `aarch64` or `arm64`, you need to use [`arm64`](https://static.getml.com/download/1.4.0/getml-1.4.0-arm64-community-edition-linux.tar.gz), otherwise go with [`x64`](https://static.getml.com/download/1.4.0/getml-1.4.0-x64-community-edition-linux.tar.gz).

```bash
# Downloads the tar file of the engine
wget https://static.getml.com/download/1.4.0/getml-1.4.0-ARCH-community-edition-linux.tar.gz

# Extracts the tar file
tar -xf getml-1.4.0-ARCH-community-edition-linux.tar.gz

# Changes directory 
cd getml-1.4.0-ARCH-community-edition-linux

# Installs the engine
./getML install
```

Alternatively, you install the API from this repository. If you do this, you also have to install the engine as indicated above:

```bash
# Changes directory 
cd src/python-api

# Installs the python API
pip3 install .
```

### Docker (for macOS, Windows or Linux)

Before the installation, make sure your system meets the following requirements:

- [Docker](https://www.docker.com/) | On Linux, make sure that you follow these post-installation steps to run docker without root rights/sudo: https://docs.docker.com/engine/install/linux-postinstall/

- Bash | Pre-installed on Linux and macOS. For Windows, we recommend [Git Bash](https://gitforwindows.org/).

- [OpenSSL](https://www.openssl.org/) | Pre-installed on most systems.

Once the requirements are met, the following steps should be followed:

1. Download the getml community edition for Docker from the following URL: https://static.getml.com/download/1.4.0/getml-1.4.0-community-edition-docker.zip

2. Extract `getml-1.4.0-community-edition-docker.zip` in a folder.

3. Make sure that Docker (the Docker daemon) is running.

4. Execute `setup.sh`. On Windows, open the folder in which the zip file was extracted and just click on `setup.sh`. On macOS and Linux, do the following:

    ``` bash
    cd getml-1.4.0-community-edition-docker
    bash setup.sh # or ./setup.sh
    ```

    This will run the Dockerfile and setup your Docker image. It will also create a Docker volume called 'getml'.

5. Execute `run.sh`. On Windows, you can just click on `run.sh`. On macOS and Linux, do the following:

    ```bash
    bash run.sh # or ./run.sh
    ```

    This will run the Docker image. 

## Deinstallation

### Linux

You will have to remove the folder `.getML` from your home directory. In a terminal, execute: 
```
rm -r $HOME/.getML
```

### Docker

On Windows, you can just click on uninstall.sh.

On macOS and Linux, execute the following in a terminal:

```
bash uninstall.sh # or ./uninstall.sh
```

## Compiling from source

Because getML is a complex software, we use Docker for our build environment. If
you want to compile it from source, we provide a set of [wrappers to ease local
development](bin):

```bash
./bin/getml
```

``` bash
Usage:
    getml <subcommand> [options]

Subcommands:
    build   Build utilities
    help    Show help (this message)

Options:
    -h      Show help (this message)
```

The `build` subcommand is the entrypoint for building getml from source. For
details about the build process, see [Directly interacting with
bake](#directly-interacting-with-bake) below.

``` bash
getml build wrapper

Usage:
  build <subcommand> [options]

Subcommands:
  [a]ll       Build all (whole package, ref. [p]ackage)
  [c]li       Build cli
  [e]ngine    Build engine
  [p]ackage   Build package

Options:
  -b <args>   Specify build args (-b KEY=VALUE); passed to docker build
  -h          Show help (this message)
  -o <path>   Set output path (default: build); passed to docker build
```

Most of the time you probably want to build the (C++) engine:

```bash
./bin/getml build engine
```

If you are calling `getml build package` all build artifacts will be packaged
inside the specified output folder. Further, a tarball
(`getml-<version>-<arch>-linux.tar.gz`) will be created inside the folder.

### Build options
#### `-b <args>`: Build args 
[Build args](https://docs.docker.com/build/guide/build-args/) passed to `docker
build`. Build args can be provided as key-value pairs. The following build args
are supported:
- `VERSION`: the build version (default: the version specified in the [`VERSION`
  file](VERSION)) 
- `NJOBS`: the number of threads to use for each compilation step
#### `-h`: Show help
Show the help screen
#### `-o <path>`: Output folder
The [output folder](https://docs.docker.com/build/exporters/) used by docker's
export backend

### Directly interacting with bake
The build pipeline is based on multi-stage docker builds. There are two `Dockerfile`s:
- one for cli, wheel and packaging located in the project root:
  [`./Dockerfile`](./Dockerfile)
- one related to the engine and its dependencies in the engine subfolder:
  [`./src/engine/Dockerfile`](./src/egnine/Dockerfile)

As the second `Dockerfile` is a dependency for the first, we use
[bake](https://docs.docker.com/build/bake/) to orchestrate the builds. The
[bake file](docker-bake.hcl) holds definitions for all build targets and
ensures the appropriate build contexts are set.

If you want to interact with docker directly, you can do so by calling `docker
buildx bake`:

``` bash
VERSION=1.5.0 docker buildx bake engine
```

If you want to override build-args, you can do so per build stage via [bake's
`--set`
overrides](https://docs.docker.com/reference/cli/docker/buildx/bake/#set):

``` bash
docker buildx bake engine --set engine.args.VERSION=1.5.0
```

This way, you can also override some of a target's [default
attributes](https://docs.docker.com/reference/cli/docker/buildx/bake/#set):

``` bash
docker buildx bake engine --set engine.output=out
```
