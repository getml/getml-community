The pipeline for **Linux**.

# Notes

For now this pipeline does not allow to run the unit test. But
whatever runs on **macOS** will most probably also pass under
Linux. As soon as we have a log in via the command line I will add
this feature.

While the compilation of the code is done inside a `Docker`
container, it's execution must - for performance reasons - run in a
native Linux environment.

## Additional commands

Some additional commands are supported in the `build.sh` script
specifically tailored to work with the Docker image.

| Command          | Description                                                                                              |
| ---------------- | -------------------------------------------------------------------------------------------------------- |
| `init_docker`    | Build the Docker image. This command will also be called within the `init` command.                      |
| `compile_docker` | Command executed within the Docker container to compile all the source code.                             |
| `package_docker` | Command executed within the Docker container to bundle all binaries and construct the resulting package. |
| `test_docker`    | Command executed within the Docker container to run the unit tests of all components of the getML suite. |
