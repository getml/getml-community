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
ENV TARGET_SYSTEM_PROCESSOR_CONAN=x86_64
ENV TARGET_TRIPLET=x64-linux-release

FROM quay.io/pypa/manylinux_2_28_aarch64:2024-06-03-e195670 as base-arm64
ENV TARGET_SYSTEM_PROCESSOR=aarch64
ENV TARGET_SYSTEM_PROCESSOR_CONAN=armv8
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
    gcc-toolset-14-gcc-c++ \
    flex \
    zip \
    perl-IPC-Cmd \
    perl-Digest-SHA \
    https://extras.getpagespeed.com/release-latest.rpm

RUN pipx install ninja


# ------------------------------------------------------------------------------
# Image with necessary tools for building the engine
FROM engine-base-tools AS engine-tools

RUN pipx install "conan==2.15.0"
ENV CONAN_P=/root/.conan2/p
ENV CONAN_BINARY=/usr/local/bin/conan
RUN mkdir -p /root/.conan2/profiles
COPY <<EOF /root/.conan2/profiles/default
[settings]
arch=${TARGET_SYSTEM_PROCESSOR_CONAN}
build_type=Release
compiler=gcc
compiler.cppstd=23
compiler.libcxx=libstdc++11
compiler.version=14
os=Linux

[conf]
tools.cmake.cmaketoolchain:generator=Ninja
EOF

RUN git clone --depth 1 --branch v4.10.2 https://github.com/ccache/ccache.git /src/ccache

RUN source scl_source enable gcc-toolset-14 \
    && cmake -DCMAKE_BUILD_TYPE=Release -S /src/ccache -B /src/ccache/build -G Ninja \
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
ENV CACHE_ID="b088bb31-d9ff-442c-a877-89dfc6a34642"

RUN mkdir -p $ENGINE_DIR $CONAN_INSTALLED_DIR
COPY $ENGINE_REPO_SOURCE/conanfile.py $ENGINE_DIR
COPY $ENGINE_REPO_SOURCE/conan-repository $ENGINE_DIR/conan-repository

WORKDIR $ENGINE_DIR

RUN --mount=type=cache,target=$CCACHE_PATH,id="$CCACHE_PATH-$CACHE_ID" \
    --mount=type=cache,target=$CONAN_P,id="$CONAN_P-$CACHE_ID",sharing=locked \
    conan remote add getml-conan-repository $ENGINE_DIR/conan-repository \
    && source scl_source enable gcc-toolset-14 \
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

RUN --mount=type=cache,target=$CCACHE_PATH,id="$CCACHE_PATH-$CACHE_ID" \
    --mount=type=cache,target=$CONAN_P,id="$CONAN_P-$CACHE_ID",sharing=locked \
    --mount=type=cache,target=$ENGINE_BUILD_PATH,id="$ENGINE_BUILD_PATH-$CACHE_ID",sharing=locked \
    if [ -n "$NJOBS" ]; then export CMAKE_BUILD_PARALLEL_LEVEL="$NJOBS"; fi \
    && source scl_source enable gcc-toolset-14 \
    && ccache --zero-stats \
    && cmake --preset ${ENGINE_PRESET} -D CMAKE_UNITY_BUILD=ON -D CMAKE_UNITY_BUILD_BATCH_SIZE=0 \
    && cmake --build --preset ${ENGINE_PRESET} \
    && ccache --show-stats -v -v


# ------------------------------------------------------------------------------
# Image with the built engine at /engine-package
FROM engine-build AS engine-package

ARG TARGETARCH
ARG PACKAGE_NAME="getml-community-$GETML_VERSION-$TARGETARCH-linux"

RUN --mount=type=cache,target=$CONAN_P,id="$CONAN_P-$CACHE_ID",sharing=locked \
    --mount=type=cache,target=$ENGINE_BUILD_PATH,id="$ENGINE_BUILD_PATH-$CACHE_ID",sharing=locked \
    mkdir -p /engine-package/$PACKAGE_NAME/ \
    && cmake --install $ENGINE_BUILD_PATH --prefix /engine-package/$PACKAGE_NAME/ --strip


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
