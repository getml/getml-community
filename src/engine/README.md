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

* [VCPKG](https://learn.microsoft.com/en-gb/vcpkg/get_started/get-started) (see [vcpkg.json](./vcpkg.json)) or
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

Using GCC >= 13

* gcc-toolset-13-gcc-c++
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

Using GCC >= 13

* gcc
* g++
* gcc-13
* g++-13
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

  "base"                                     - Base
  "release"                                  - Release
  "release-native"                           - Release native
  "release-original"                         - Release (original)
  "release-vcpkg"                            - Release (vcpkg)
  "release-native-vcpkg"                     - Release native (vcpkg)
  "release-original-vcpkg"                   - Release (original) (vcpkg)
  "release-vcpkg-x86_64-linux"               - Release (x86_64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux"              - Release (aarch64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux-crosscompile" - Release (aarch64-linux) (vcpkg) (crosscompile)
  "release-conan"                            - Release (conan)
  "debug"                                    - Debug
  "debug-full"                               - Debug with full information
  "debug-vcpkg"                              - Debug (vcpkg)
  "debug-full-vcpkg"                         - Debug with full information (vcpkg)
  "debug-vcpkg-aarch64-linux"                - Debug (aarch64-linux) (vcpkg)
  "debug-conan"                              - Debug (conan)
```


### Possible `build` presets

```bash
$ cmake --list-presets=build
Available build presets:

  "release"                                  - Release
  "release-native"                           - Release native
  "release-original"                         - Release (original)
  "release-vcpkg"                            - Release (vcpkg)
  "release-native-vcpkg"                     - Release native (vcpkg)
  "release-original-vcpkg"                   - Release original (vcpkg)
  "release-vcpkg-x86_64-linux"               - Release (x86_64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux"              - Release (aarch64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux-crosscompile" - Release (aarch64-linux) (vcpkg) (crosscompile)
  "release-conan"                            - Release (conan)
  "debug"                                    - Debug
  "debug-full"                               - Debug with full information
  "debug-vcpkg"                              - Debug (vcpkg)
  "debug-full-vcpkg"                         - Debug full (vcpkg)
  "debug-vcpkg-aarch64-linux"                - Debug (aarch64-linux) (vcpkg)
  "debug-conan"                              - Debug (conan)
```


### Possible `test` presets

```bash
$ cmake --list-presets=test 
Available test presets:                                                       
                                                                              
  "release"     - Release                                                     
  "debug"       - Debug                                                       
  "debug-vcpgk" - Debug (vcpkg)                                               
  "debug-conan" - Debug (conan)                                               
```


### Possible `package` presets

```bash
$ cmake --list-presets=package
Available package presets:

  "release"                                  - Release
  "release-native"                           - Release native
  "release-original"                         - Release original
  "release-vcpkg"                            - Release (vcpkg)
  "release-native-vcpkg"                     - Release native (vcpkg)
  "release-original-vcpkg"                   - Release original (vcpkg)
  "release-vcpkg-x86_64-linux"               - Release (x86_64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux"              - Release (aarch64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux-crosscompile" - Release (aarch64-linux) (vcpkg) (crosscompile)
  "release-conan"                            - Release (conan)
```


### Possible `workflow` presets

```bash
$ cmake --list-presets=workflow
Available workflow presets:

  "release"                                  - Release
  "release-native"                           - Release native
  "release-original"                         - Release (original)
  "release-vcpkg"                            - Release (vcpkg)
  "release-native-vcpkg"                     - Release native (vcpkg)
  "release-original-vcpkg"                   - Release (original) (vcpkg)
  "release-vcpkg-x86_64-linux"               - Release (x86_64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux"              - Release (aarch64-linux) (vcpkg)
  "release-conan"                            - Release (conan)
  "debug"                                    - Debug
  "debug-full"                               - Debug with full information
  "debug-vcpkg"                              - Debug (vcpkg)
  "debug-full-vcpkg"                         - Debug with full debug information (vcpkg)
  "debug-vcpkg-aarch64-linux"                - Debug (aarch64-linux) (vcpkg)
  "release-vcpkg-aarch64-linux-crosscompile" - Release (aarch64-linux) (vcpkg) (crosscompile)
  "debug-conan"                              - Debug (conan)
```


## How To Build using CMake WorkFlows

A CMake workflow consists at least of a `configuration` and can have steps for `build`, `test`, `package`.


### Using system packages

```bash
$ cmake --workflow --preset debug
$ cmake --workflow --preset release
```


### Using VCPKG

Make sure to install vcpkg: [https://vcpkg.io/](https://vcpkg.io/)

```bash
$ VCPKG_ROOT=$PATH_TO/vcpkg cmake --workflow --preset debug-vcpkg
$ VCPKG_ROOT=$PATH_TO/vcpkg cmake --workflow --preset release-vcpkg
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

The current available preset for it is `release-vcpkg-x86_64-linux`

```bash
$ VCPKG_ROOT=$PATH_TO/vcpkg cmake --workflow --preset release-vcpkg-x86_64-linux
```


## Troubleshooting

### Setting VCPKG_ROOT

After the initial CMake configuration ran, the `VCPKG_ROOT` variable is set in the cache and does not need to be set again.

Instead of setting `VCPKG_ROOT` each time a new configuration is choosen, it can be set in the environment.

See [Tutorial: Install and use packages with vcpkg - Configure the `VCPKG_ROOT` environment variable.](https://learn.microsoft.com/en-us/vcpkg/get_started/get-started?pivots=shell-bash#2---set-up-the-project)

```bash
$ export VCPKG_ROOT=$PATH_TO/vcpkg
$ cmake --workflow --preset debug-vcpkg
```


### Faster linking with Gold, LLD and Mold

To speed up link time, for example the following linkers can be used

- [`gold`](https://sourceware.org/binutils/)
- [`ldd`](https://lld.llvm.org/)
- [`mold`](https://github.com/rui314/mold)

```bash
$ cmake --preset debug-vcpkg -DCMAKE_LINKER_TYPE=GOLD
$ cmake --preset debug-vcpkg -DCMAKE_LINKER_TYPE=LLD
$ cmake --preset debug-vcpkg -DCMAKE_LINKER_TYPE=MOLD
```

This works since [CMake 3.29](https://cmake.org/cmake/help/latest/variable/CMAKE_LINKER_TYPE.html).

Older versions of CMake can use the `CMAKE_EXE_LINKER_FLAGS` and `CMAKE_SHARED_LINKER_FLAGS` variable and pass the linker flags directly.

See ['How to try out the new mold linker with your CMake C/C++ project'](https://gist.github.com/MawKKe/b8af6c1555f1c7aa4c2760350ed97fff).

```bash
$ cmake --preset debug-vcpkg -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=gold -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=gold"
$ cmake --preset debug-vcpkg -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=lld -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=lld"
$ cmake --preset debug-vcpkg -DCMAKE_EXE_LINKER_FLAGS="-fuse-ld=mold -DCMAKE_SHARED_LINKER_FLAGS="-fuse-ld=mold"
```

Especially `mold` is very fast and can be used as a drop-in replacement for `ld` while developing.


### Limiting the number of jobs or theads used by the build

Use the environment variable `CMAKE_BUILD_PARALLEL_LEVEL` to limit the number of jobs or threads used by the build.

```bash
$ CMAKE_BUILD_PARALLEL_LEVEL=4 cmake --workflow --preset debug-vcpkg
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


#### libpq

`libpq` from VCPKG needs `zic` to be installed. This is part of the Debian package `libc-bin` and resides under `/usr/sbin/`. Make sure it is in your `PATH`.

```bash
$ PATH=/usr/sbin/:$PATH VCPKG_ROOT=$PATH_TO/vcpkg cmake --preset debug-vcpkg
```


### Cross-Compiling

VCPKG provides triplets like `x86-mingw-dynamic.cmake` and `arm64-linux-release.cmake`, which can be used in the `VCPKG_TARGET_TRIPLET` variable.

Sadly, some ports are not directly compatible and need either adjusting the system, or the port.

Problems are for example

- case-sensitive include names, e.g. Windows.h, Shlwapi.h, WS2tcpip.h, AccCtrl.h, Aclapi.h
- a hard requirement on `mc.exe` instead of accepting `x86_64-w64-mingw32-windmc` via `CMAKE_MC_COMPILER`
- [needing powershell to copy tools](https://github.com/microsoft/vcpkg/blob/master/scripts/cmake/vcpkg_copy_tool_dependencies.cmake#L29C1-L29C51)
- having trouble with certain `__asm__` calls


#### Example: Cross-Compiling for ARM64-/AARCH64-Linux

An example for cross-compiling for ARM64-/AARCH64-Linux using VCPKG is provided in the `release-vcpkg-aarch64-linux-crosscompile` preset.

As it uses a cross-compilation version of GCC, which makes it similar fast as a native build.

For packaging using CPack, it fails because it can't find

```bash
$ VCPKG_ROOT=$PATH_TO/vcpkg GETML_VERSION=1.5.0 cmake --workflow --preset release-vcpkg-aarch64-linux-crosscompile
```

> [!IMPORTANT]  
> The `package` step is not working for cross-compilation. It failes because it can't find the necessary libraries.  
> To fix this, the `BUILD_RPATH` must be set to the path where the libraries are located.

```cmake
# src/CMakeLists.txt
set_target_properties(
  engine
  PROPERTIES
    # Adding the BUILD_RPATH to make packaging find the libraries
    BUILD_RPATH "/usr/aarch64-linux-gnu/lib"
    INSTALL_RPATH "$ORIGIN/../lib"
    RUNTIME_OUTPUT_DIRECTORY bin
)
```

```bash
$ cpack --preset release-vcpkg-aarch64-linux-crosscompile
```


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
$ VCPKG_ROOT=$PATH_TO/vcpkg cmake --preset debug-vcpkg
$ ln -s build/engine-debug-vcpkg-build/compile_commands.json compile_commands.json
```
