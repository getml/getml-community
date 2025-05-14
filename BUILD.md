# BUILD.md

## Building getML from Source

getML is a complex software package, and to simplify the build process, we use Docker for our build environment. This guide will walk you through setting up, building, and interacting with getML. Whether you are targeting a single platform or building for multiple architectures, follow the instructions below to compile getML from source.

### Prerequisites

- [Docker](https://docs.docker.com/get-started/get-docker) installed and running on your system.
- [Docker Buildx](https://github.com/docker/buildx/blob/master/README.md#installing) installed for multi-platform builds.
- Optionally, ensure [QEMU](https://docs.docker.com/build/building/multi-platform/#qemu) is set up for cross-platform builds.

Verify if Docker Buildx is installed by running:

```bash
docker buildx version
```

### Enabling Multi-Platform Support

#### Docker Desktop

1. Open **Settings**.
2. In the **General** tab, enable **Use containerd for pulling and storing images**.

#### Docker Engine (non-Docker Desktop)

1. Add the following to `/etc/docker/daemon.json`:

   ```json
   {
     "features": {
       "containerd-snapshotter": true
     }
   }
   ```

2. Restart Docker:

   ```bash
   sudo systemctl restart docker
   ```

For more details, refer to Docker's [multi-platform guide](https://docs.docker.com/build/building/multi-platform/#enable-the-containerd-image-store).

---

## Quick Start: Local Development

To simplify development and manage build processes, we provide a script-based interface. Begin by navigating to the `scripts` directory and using the `getml` wrapper to initiate the build process:

```bash
./scripts/build
```

### Available Subcommands

| Subcommand  | Description                                                  |
| ----------- | ------------------------------------------------------------ |
| `build`     | Build utilities                                               |
| `help`      | Show help for available commands                              |

Within the `build` subcommand, you can build various components of getML:

```bash
./scripts/build <subcommand> [options]
```

| Subcommand               | Description                                                                              |
|--------------------------|------------------------------------------------------------------------------------------|
| `all`                    | Build the entire package, including CLI, Engine, Python API, and tarball archive.        |
| `cli`                    | Build the CLI (Command Line Interface).                                                  |
| `engine`                 | Build the C++ Engine.                                                                    |
| `package`                | Export runnable Engine + CLI package.                                                    |
| `python`                 | Package the Python API.                                                                  |
| `python-copy-artifacts`  | Package the Python API, copying engine build artifacts.                                  |
| `docker`                 | Build the Docker runtime image.                                                          |
| `docker-copy-artifacts`  | Build the Docker runtime image, copying the engine build artifacts.                      |
| `archive`                | Create a tarball (`.tar.gz`) archive of the package.                                     |
| `archive-copy-artifacts` | Create a tarball (`.tar.gz`) archive of the package, copying the engine build artifacts. |

#### Common Build Options

> [!NOTE]
> Sometimes, the CMake build preset is cached in dangling state. [If you want to reset the CMake build preset](src/engine/README.md#build-using-docker), set `GETML_CMAKE_FRESH_FRESET=true`:
> 
> ```bash
> GETML_CMAKE_FRESH_PRESET=true ./scripts/build <subcommand> [options]
> ```

| Option         | Description                                                               |
| -------------- | ------------------------------------------------------------------------- |
| `-b <args>`    | Specify build arguments (e.g., `-b VERSION=1.5.0`) to be passed to Docker. |
| `-h`           | Display help information.                                                 |
| `-o <path>`    | Set the output path (default: `build`).                                   |
| `-p <platform>`| Set the target platform (`linux/amd64`, `linux/arm64`), default: native.   |

For most cases, you may want to build the Engine:

```bash
./scripts/build engine
```

If you are building the entire package for distribution:

```bash
./scripts/build package
```

This command will generate all the required artifacts, which will be placed in the output folder. To create a compressed tarball for distribution, use the `archive` subcommand:

```bash
./scripts/build archive
```

---

## Multi-platform Builds

By default, the target is only built for the native platform (`BUILDPLATFORM`). If you want to also build the target for the foreign platform you need to explicitly set the target's [`platform` argument](https://docs.docker.com/reference/cli/docker/buildx/build/#platform). The `build` wrapper supplies the shorthand option `-p` to do so. E.g. for the whole package:

```bash
./scripts/build package -p linux/arm64,linux/amd64
```

For specifics on multiplatform builds regarding the Docker runtime, see [runtime/README.md](./runtime/README.md#multiplatform-builds).

---

## Advanced: Interacting Directly with Docker

If you prefer interacting directly with Docker, the build pipeline uses multi-stage Docker builds, orchestrated with [Docker Bake](https://docs.docker.com/build/bake/). There are two main `Dockerfile`s:

- **CLI and Python packaging**: [`./Dockerfile`](./Dockerfile)
- **Engine and dependencies**: [`./src/engine/Dockerfile`](./src/engine/Dockerfile)

To build specific targets using Docker Bake:

```bash
VERSION=1.5.0 docker buildx bake engine
```

To override specific build arguments:

```bash
docker buildx bake engine --set engine.args.VERSION=1.5.0
```

For more information, refer to Docker's [Bake documentation](https://docs.docker.com/reference/cli/docker/buildx/bake/).

---

## Running the Docker Image

After building the runtime, you can run the Docker image:

```bash
docker run -it getml/getml -p 1708-1733:11708-11733
```

---

## Troubleshooting

- **Docker Build Fails**: Ensure that Docker and Docker Buildx are installed and properly configured. For multi-platform builds, confirm that QEMU is set up for cross-platform emulation.
- **Missing Artifacts**: Ensure that all required build artifacts for the target platforms are present in the output directory before running a multi-platform build.
- **Permission Errors**: If you encounter permission issues, try running commands with `sudo`, or ensure your user has proper permissions to interact with Docker.

For additional help, consult the [getML documentation](https://getml.com) or raise an [issue](https://github.com/getml/getml-community/issues) in the repository.
