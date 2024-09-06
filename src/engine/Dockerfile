# syntax=docker/dockerfile:1.4

ARG ENGINE_REPO_SOURCE="."
ARG ENGINE_DIR="/src/engine"


# ------------------------------------------------------------------------------
# Base images
# Tags are available at:
# - https://quay.io/repository/pypa/manylinux_2_28_x86_64?tab=tags
# - https://quay.io/repository/pypa/manylinux_2_28_aarch64?tab=tags
FROM quay.io/pypa/manylinux_2_28_x86_64:2024-06-03-e195670 as base-amd64
ENV TARGET_SYSTEM_PROCESSOR=x86_64
ENV TARGET_TRIPLET=x64-linux-release

FROM quay.io/pypa/manylinux_2_28_aarch64:2024-06-03-e195670 as base-arm64
ENV TARGET_SYSTEM_PROCESSOR=aarch64
ENV TARGET_TRIPLET=aarch64-linux-release

FROM base-${TARGETARCH} as base


# ------------------------------------------------------------------------------
# Base image with necessary tools and system libraries
FROM base AS engine-base-tools
RUN dnf groupinstall -y "Development Tools"
RUN dnf install -y \
    readline-devel \
    openssl-devel \
    bison \
    zlib-devel \
    gcc-toolset-13-gcc-c++ \
    flex \
    zip \
    perl-IPC-Cmd \
    perl-Digest-SHA \
    https://extras.getpagespeed.com/release-latest.rpm

RUN pipx install ninja


# ------------------------------------------------------------------------------
# Adding current GCC to Image
# GCC 13.2.1 is the default compiler in the manylinux images
# but it creates a virus warning with mariadb-connector-c
FROM engine-base-tools AS engine-gcc-tools
ENV GCC_VERSION=14.2.0
ENV GCC_HOME=/opt/gcc/gcc-toolset-14
RUN mkdir -p $GCC_HOME
RUN git clone --depth 1 --branch releases/gcc-$GCC_VERSION git://gcc.gnu.org/git/gcc.git /src/gcc
RUN mkdir -p /src/gcc-build

WORKDIR /src/gcc
RUN ./contrib/download_prerequisites

WORKDIR /src/gcc-build
RUN /src/gcc/configure --prefix=$GCC_HOME --enable-languages=c,c++,fortran --disable-multilib
RUN make -j$(nproc)
RUN make install
RUN rm -r /src/gcc /src/gcc-build

ENV CC=$GCC_HOME/bin/gcc
ENV CXX=$GCC_HOME/bin/g++
ENV LD_LIBRARY_PATH=$GCC_HOME/lib64:$LD_LIBRARY_PATH


# ------------------------------------------------------------------------------
# Image with necessary tools for building the engine
FROM engine-gcc-tools AS engine-tools

RUN pipx install "conan==2.7.0"
ENV CONAN_P=/root/.conan2/p
ENV CONAN_BINARY=/usr/local/bin/conan
RUN mkdir -p /root/.conan2/profiles
COPY <<EOF /root/.conan2/profiles/default
[settings]
arch=x86_64
build_type=Release
compiler=gcc
compiler.cppstd=23
compiler.libcxx=libstdc++11
compiler.version=14
os=Linux

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
tools.build:compiler_executables={'c': '$GCC_HOME/bin/gcc', 'cpp': '$GCC_HOME/bin/g++'}
EOF

RUN git clone --depth 1 --branch v4.10.2 https://github.com/ccache/ccache.git /src/ccache
RUN cmake -DCMAKE_BUILD_TYPE=Release -S /src/ccache -B /src/ccache/build -G Ninja -D CMAKE_INSTALL_RPATH=$GCC_HOME/lib64 \
    && cmake --build /src/ccache/build \
    && cmake --install /src/ccache/build \
    && rm -r /src/ccache

ENV CMAKE_CXX_COMPILER_LAUNCHER=ccache CMAKE_C_COMPILER_LAUNCHER=ccache
ENV CCACHE_PATH="/root/.cache/ccache"
ENV CONAN_INSTALLED_DIR="/src/conan_installed"


# ------------------------------------------------------------------------------
# Image with necessary library dependencies
FROM engine-tools AS engine-dependencies
ARG ENGINE_REPO_SOURCE
ARG ENGINE_DIR

RUN mkdir -p $ENGINE_DIR $CONAN_INSTALLED_DIR
COPY $ENGINE_REPO_SOURCE/conanfile.py $ENGINE_DIR
COPY $ENGINE_REPO_SOURCE/conan-repository $ENGINE_DIR/conan-repository

WORKDIR $ENGINE_DIR

RUN --mount=type=cache,target=$CCACHE_PATH,id="$CCACHE_PATH-b088bb31-d9ff-442c-a877-89dfc6a34642" \
    --mount=type=cache,target=$CONAN_P,id="$CONAN_P-b088bb31-d9ff-442c-a877-89dfc6a34642",sharing=locked \
    conan remote add getml-conan-repository $ENGINE_DIR/conan-repository \
    && conan install $ENGINE_DIR --output-folder=$CONAN_INSTALLED_DIR --build missing --settings build_type=Release \
    && conan remote remove getml-conan-repository


# ------------------------------------------------------------------------------
# Image building the engine
FROM engine-dependencies AS engine-build

ARG VERSION="0.0.0"
ARG NJOBS
ENV GETML_VERSION=$VERSION
ARG ENGINE_PRESET="release-conan"
ARG ENGINE_REPO_SOURCE
ARG ENGINE_DIR
ENV ENGINE_BUILD_PATH="$ENGINE_DIR/build/release-conan-build"

COPY $ENGINE_REPO_SOURCE $ENGINE_DIR

WORKDIR $ENGINE_DIR
ENV CMAKE_BUILD_RPATH=$GCC_HOME/lib64
ENV CPACK_EXCLUDE_RUNTIME_DEPENDENCIES=/lib64/libgcc_s.so.1;/lib64/libstdc++.so.6;/lib64/libgomp.so.1

RUN --mount=type=cache,target=$CCACHE_PATH,id="$CCACHE_PATH-b088bb31-d9ff-442c-a877-89dfc6a34642" \
    --mount=type=cache,target=$CONAN_P,id="$CONAN_P-b088bb31-d9ff-442c-a877-89dfc6a34642",sharing=locked \
    --mount=type=cache,target=$ENGINE_BUILD_PATH,id="$ENGINE_BUILD_PATH-b088bb31-d9ff-442c-a877-89dfc6a34642",sharing=locked \
    if [ -n "$NJOBS" ]; then export CMAKE_BUILD_PARALLEL_LEVEL="$NJOBS"; fi \
    && ccache --zero-stats \
    && cmake --workflow --preset ${ENGINE_PRESET} \
    && ccache --show-stats -v -v


# ------------------------------------------------------------------------------
# Image with the built engine at /engine-package
FROM engine-build AS engine-package

ARG TARGETARCH
ARG PACKAGE_NAME="getml-$GETML_VERSION-$TARGETARCH-linux"
ARG ENGINE_PACKAGE_TGZ="engine-$GETML_VERSION-Linux-$TARGET_SYSTEM_PROCESSOR.tar.gz"

RUN --mount=type=cache,target=$ENGINE_BUILD_PATH,id="$ENGINE_BUILD_PATH-b088bb31-d9ff-442c-a877-89dfc6a34642",sharing=locked \
    mkdir -p /engine-package/$PACKAGE_NAME/ \
    && tar -xzf $ENGINE_BUILD_PATH/package/$ENGINE_PACKAGE_TGZ -C /engine-package/$PACKAGE_NAME/ --strip-components=1


# ------------------------------------------------------------------------------
# Image with the final package for output to host
FROM scratch AS package
COPY --from=engine-package /engine-package/ /


# ------------------------------------------------------------------------------
# Image testing running the engine - on top of almalinux
FROM almalinux:9.4-minimal AS engine-run-almalinux
RUN mkdir /opt/getml
COPY --from=engine-package /engine-package/* /opt/getml
COPY <<EOF /opt/getml/config.json
    {
        "engine": {
            "port": 1708
        },
        "monitor": {
            "allowPushNotifications": true,
            "allowRemoteIPs": false,
            "launchBrowser": true,
            "log": false,
            "httpPort": 1709,
            "proxyUrl": "",
            "tcpPort": 1711
        },
        "inMemory": true,
        "projectDirectory": "../projects/"
    }
EOF
WORKDIR /opt/getml/bin
CMD ["./engine" , "--project=QUICK_TEST"]


# ------------------------------------------------------------------------------
# Image testing running the engine - on top of debian
FROM debian:12.5-slim AS engine-run-debian
RUN mkdir /opt/getml
COPY --from=engine-package /engine-package/* /opt/getml
COPY <<EOF /opt/getml/config.json
    {
        "engine": {
            "port": 1708
        },
        "monitor": {
            "allowPushNotifications": true,
            "allowRemoteIPs": false,
            "launchBrowser": true,
            "log": false,
            "httpPort": 1709,
            "proxyUrl": "",
            "tcpPort": 1711
        },
        "inMemory": true,
        "projectDirectory": "../projects/"
    }
EOF
WORKDIR /opt/getml/bin
CMD ["./engine" , "--project=QUICK_TEST"]
