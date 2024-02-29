
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
`uname -m` to figure out the architecture. If it says something like `aarch64` or `arm64`, you need to use `arm64`, otherwise go with `x64`.

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

Because getML is a complicated software, we use Docker
for our build environment. If you want to compile it from source,
here is what you have to do:

On a Linux machine, install Docker (if you don't have a Linux
machine, we recommend https://multipass.run):

https://docs.docker.com/engine/install/ubuntu/

Also make sure that you follow the post-installation steps for Linux, so
can launch Docker without root rights:

https://docs.docker.com/engine/install/linux-postinstall/

Then, do the following, again replacing `ARCH` with either `x64` or `arm64`:

```bash
# Go to the linux folder inside the repository
cd linux-ARCH

# Set up the Docker container (you only have to do this once)
./build.sh init_docker

# Download and compile the dependencies - this will
# take a while (you only have to do this once)
./build.sh init

# Builds the package
./build.sh p

# Runs the newly built engine
./build.sh x
```

From now on, whenever you want to build a new
package, you can do that with one simple command:

```bash
./build.sh p
```

If you want to change the C++ code and then compile only the engine,
you can do the following:

```bash
./build.sh c
```

Likewise, if you want to change the Go code and then compile only the entrypoint,
you can do the following:

```bash
./build.sh app
```