# GetML Engine


## How To Build

A build can either be done by triggering the complete workflow

```bash
$ cmake --workflow --preset $PRESET_NAME
```

or by executing separate steps

```bash
$ cmake --preset $PRESET_NAME
$ cmake --build --preset $PRESET_NAME
$ ctest --preset $PRESET_NAME
$ cpack --preset $PRESET_NAME
```

Subsequent builds, tests, packaging, can be done by calling the right action and preset combination

```bash
$ cmake --build --preset $PRESET_NAME
```


### Requirements

For dependencies either

* [Conan](https://docs.conan.io/2/installation.html) (see [conanfile.py](./conanfile.py)) or
* system packages (check [CMakeLists.txt](CMakeLists.txt)) can be used.


#### AlmaLinux

* readline-devel
* openssl-devel
* bison
* zlib-devel
* flex
* zip
* perl-IPC-Cmd

Using GCC >= 14

* gcc-toolset-14-gcc-c++
* libgomp-devel

Using Clang >= 18

* llvm-toolset >= 18
* libomp-devel >= 18

From external

* [ninja-build](https://github.com/ninja-build/ninja/) (for faster builds)


#### Debian

* cmake
* ninja-build
* git
* curl
* zip
* unzip
* tar
* pkg-config
* flex
* bison
* autoconf
* libtool

Using GCC >= 14

* gcc
* g++
* gcc-14
* g++-14
* libgomp1

Using Clang >= 18

* clang
* clang-18
* libc++-18-dev
* libc++abi-18-dev
* libomp-18-dev
* libomp-dev

Extras

* pipx (e.g. for conan and ninja-build)
* ccache (for faster re-builds)
* mold (for faster linking)
* clang-tidy (for static code analysis)
* clang-format (for code formatting)


### Possible `configuration` presets

```bash
$ cmake --list-presets
Available configure presets:

  "base"             - Base
  "release"          - Release
  "release-native"   - Release native
  "release-original" - Release (original)
  "release-conan"    - Release (conan)
  "debug"            - Debug
  "debug-full"       - Debug with full information
  "debug-conan"      - Debug (conan)
```


### Possible `build` presets

```bash
$ cmake --list-presets=build
Available build presets:

  "release"          - Release
  "release-native"   - Release native
  "release-original" - Release (original)
  "release-conan"    - Release (conan)
  "debug"            - Debug
  "debug-full"       - Debug with full information
  "debug-conan"      - Debug (conan)
```


### Possible `test` presets

```bash
$ cmake --list-presets=test
Available test presets:

  "release"     - Release
  "debug"       - Debug
  "debug-conan" - Debug (conan)
```


### Possible `package` presets

```bash
$ cmake --list-presets=package
Available package presets:

  "release"          - Release
  "release-native"   - Release native
  "release-original" - Release original
  "release-conan"    - Release (conan)
```


### Possible `workflow` presets

```bash
$ cmake --list-presets=workflow
Available workflow presets:

  "release"          - Release
  "release-native"   - Release native
  "release-original" - Release (original)
  "release-conan"    - Release (conan)
  "debug"            - Debug
  "debug-full"       - Debug with full information
  "debug-conan"      - Debug (conan)
```


## How To Build using CMake WorkFlows

A CMake workflow consists at least of a `configuration` and can have steps for `build`, `test`, `package`.


### Using system packages

```bash
$ cmake --workflow --preset debug
$ cmake --workflow --preset release
```


### Using CONAN

Make sure to install conan: [https://conan.io/](https://conan.io/)

```bash
$ CONAN_BINARY=$(which conan) cmake --workflow --preset debug-conan
$ CONAN_BINARY=$(which conan) cmake --workflow --preset release-conan
```

For setting a specific profile, use the `CONAN_PROFILE` variable. The profile must be available in the conan profiles
under `~/.conan2/profiles/`.

```bash
$ CONAN_BINARY=$(which conan) CONAN_PROFILE=debug-clang18 cmake --workflow --preset debug-conan
$ CONAN_BINARY=$(which conan) CONAN_PROFILE=release-gcc cmake --workflow --preset release-conan
```


### Creating a publishable release

To get a publishable release package, the right combination of flags and libraries must be used.

The current available preset for it is `release-conan-x86_64-linux`

```bash
$ CONAN_BINARY=$(which conan) cmake --workflow --preset release-conan-x86_64-linux
```


## Troubleshooting

### Faster linking with Gold, LLD and Mold

To speed up link time, for example the following linkers can be used

- [`gold`](https://sourceware.org/binutils/)
- [`ldd`](https://lld.llvm.org/)
- [`mold`](https://github.com/rui314/mold)

```bash
$ cmake --preset debug-conan -DCMAKE_LINKER_TYPE=GOLD
$ cmake --preset debug-conan -DCMAKE_LINKER_TYPE=LLD
$ cmake --preset debug-conan -DCMAKE_LINKER_TYPE=MOLD
```

This works since [CMake 3.29](https://cmake.org/cmake/help/latest/variable/CMAKE_LINKER_TYPE.html).

Older versions of CMake can use the `CMAKE_EXE_LINKER_FLAGS` and `CMAKE_SHARED_LINKER_FLAGS` variable and pass the linker flags directly.

See ['How to try out the new mold linker with your CMake C/C++ project'](https://gist.github.com/MawKKe/b8af6c1555f1c7aa4c2760350ed97fff).

```bash
$ cmake --preset debug-conan -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=gold" -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=gold"
$ cmake --preset debug-conan -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=lld" -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=lld"
$ cmake --preset debug-conan -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=mold" -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=mold"
```

Especially `mold` is very fast and can be used as a drop-in replacement for `ld` while developing.


### Limiting the number of jobs or theads used by the build

Use the environment variable `CMAKE_BUILD_PARALLEL_LEVEL` to limit the number of jobs or threads used by the build.

```bash
$ CMAKE_BUILD_PARALLEL_LEVEL=4 cmake --workflow --preset debug-conan
```

This is especially useful when building on a machine with limited resources.

For Docker, this can be achieved by setting the `--build-arg` flag with the `NJOBS` variable.

```bash
$ docker build \
  -f Dockerfile \
  --target package \
  --build-arg VERSION=1.5.0 \
  --build-arg NJOBS=4 \
  --output type=local,dest=$(pwd)/build-docker \
  .
```


### Debian

#### OpenMP

To handle OpenMP and <omp.h>

- clang needs `libomp-$VERSION-dev`

- gcc needs `libgcc-$VERSION-dev`


### Cross-Compiling

Cross-Compiling for ARM64-/AARCH64-Linux from AMD64-/X86_64-Linux can be done by setting the compiler and the target or by cross-building with Docker.

Cross-Compiling on the system, e.g. via `aarch64-linux-gnu-g++` from GCC and [`clang++ --target=aarch64-linux-gnu` from Clang](https://clang.llvm.org/docs/CrossCompilation.html), and [utilizing conan](https://docs.conan.io/2/tutorial/consuming_packages/cross_building_with_conan.html#consuming-packages-cross-building-with-conan) can give near native compile performance.


#### Example: Cross-Compiling for ARM64-/AARCH64-Linux using Docker

The `Dockerfile` provides a way to build the engine for ARM64-/AARCH64-Linux using Docker.

Its cross-compilation is much slower than the native build, but it is more reliable.

```bash
$ docker build \
  -f Dockerfile \
  --target package \
  --build-arg VERSION=1.5.0 \
  --platform linux/arm64 \
  --output type=local,dest=$(pwd)/build-docker \
  .
```


## Build using Docker

Build the engine inside a Docker-Container and retrieve the resulting binaries in `./build-docker`

```bash
$ docker build \
  -f Dockerfile \
  --target package \
  --build-arg VERSION=1.5.0 \
  --output type=local,dest=$(pwd)/build-docker \
  .
```


### Quick check of Docker built engine

To quickly check the built engine, use the `engine-run-debian` or `engine-run-almalinux` targets.

```bash
$ docker build \
  -f Dockerfile \
  --target engine-run-debian \
  --tag engine-run-debian \
  --build-arg VERSION=1.5.0 \
  .

$ docker run -it engine-run-debian
```


## Running Tests

For running tests, first they need to be enabled and built. Then they can be ran via `ctest`.

```bash
$ cmake --preset debug-conan -DBUILD_TESTS=ON
$ cmake --build --preset debug-conan
$ ctest --preset debug-conan
```

Alternatively, each test suite can be executed from its own executable.

```bash
$ ./build/debug-conan-build/test/unit/fct/test_to
```

On failures, GDB, LLDB or other debuggers can be used.

```bash
$ gdb --tui --args ./build/debug-conan-build/test/unit/fct/test_to --gtest_break_on_failure
$ lldb ./build/debug-conan-build/test/unit/fct/test_to -- --gtest_break_on_failure
```

### Code Coverage

To generate code coverage, the `lcov` and `genhtml` tools can be used as follows.

1) Add coverage flags to the build of the tests.

```cmake
# test/CMakeLists.txt
# ...
  target_compile_options(${target} PRIVATE --coverage)
  target_link_options(${target} PRIVATE --coverage)
# ...
```

2) Run the tests and generate the coverage report.

```bash
$ lcov --zerocounters --directory .
$ ctest --preset debug-conan
$ lcov --capture --directory . --output-file coverage.info --ignore-errors inconsistent
$ genhtml coverage.info --output-directory coverage
$ python3 -m http.server --directory coverage
```

3) Open the browser at [http://localhost:8000](http://localhost:8000) to see the coverage report.


## Set up for LSP (Language Server Protocol) e.g. clangd

To use the LSP, a `compile_commands.json` file is needed. This can be generated by CMake.

`CMAKE_EXPORT_COMPILE_COMMANDS=ON` must be set during the configuration step.

It is enabled in debug presets.

```bash
$ CONAN_BINRAY=$(which conan) cmake --preset debug-conan
$ ln -s build/engine-debug-conan-build/compile_commands.json compile_commands.json
```
