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
FROM base AS engine-tools
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
    https://extras.getpagespeed.com/release-latest.rpm

RUN pipx install ninja

RUN git clone --depth 1 --branch v4.10 https://github.com/ccache/ccache.git /src/ccache
RUN source scl_source enable gcc-toolset-13 \
    && cmake -DCMAKE_BUILD_TYPE=Release -S /src/ccache -B /src/ccache/build -G Ninja \
    && cmake --build /src/ccache/build \
    && cmake --install /src/ccache/build \
    && rm -r /src/ccache

ENV CMAKE_CXX_COMPILER_LAUNCHER=ccache CMAKE_C_COMPILER_LAUNCHER=ccache

ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_ROOT="/src/vcpkg"
RUN git clone --branch "2024.06.15" https://github.com/microsoft/vcpkg $VCPKG_ROOT \
    && /src/vcpkg/bootstrap-vcpkg.sh -disableMetrics


WORKDIR /root


# ------------------------------------------------------------------------------
# Image with necessary library dependencies
FROM engine-tools AS engine-dependencies
ARG ENGINE_REPO_SOURCE
ARG ENGINE_DIR
ENV CCACHE_PATH="/root/.cache/ccache"
ENV ENGINE_BUILD_PATH="$ENGINE_DIR/build/release-vcpkg-${TARGET_SYSTEM_PROCESSOR}-linux-build"
ARG VCPKG_INSTALLED_PATH="$ENGINE_BUILD_PATH/vcpkg_installed"

RUN mkdir -p $ENGINE_DIR $ENGINE_DIR/overlay-ports $ENGINE_DIR/overlay-triplets
COPY $ENGINE_REPO_SOURCE/vcpkg.json $ENGINE_REPO_SOURCE/vcpkg-configuration.json $ENGINE_DIR
COPY $ENGINE_REPO_SOURCE/overlay-ports $ENGINE_DIR/overlay-ports
COPY $ENGINE_REPO_SOURCE/overlay-triplets $ENGINE_DIR/overlay-triplets

WORKDIR $ENGINE_DIR

RUN --mount=type=cache,target=$CCACHE_PATH \
    --mount=type=cache,target=$VCPKG_ROOT/buildtrees,sharing=locked \
    --mount=type=cache,target=$VCPKG_ROOT/downloads,sharing=locked \
    --mount=type=cache,target=$VCPKG_ROOT/installed,sharing=locked \
    --mount=type=cache,target=$VCPKG_ROOT/packages,sharing=locked \
    --mount=type=cache,target=/root/.cache/vcpkg,sharing=locked \
    --mount=type=cache,target=$ENGINE_BUILD_PATH,sharing=locked \
    source scl_source enable gcc-toolset-13 \
    && export CMAKE_C_COMPILER_LAUNCHER=ccache CMAKE_CXX_COMPILER_LAUNCHER=ccache \
    && $VCPKG_ROOT/vcpkg install --triplet=$TARGET_TRIPLET --x-install-root=$VCPKG_INSTALLED_PATH


# ------------------------------------------------------------------------------
# Image building the engine
FROM engine-dependencies AS engine-build

ENV VCPKG_INSTALL_OPTIONS="--no-downloads --only-binarycaching"
ARG VERSION="0.0.0"
ARG NJOBS
ENV GETML_VERSION=$VERSION
ARG ENGINE_PRESET="release-vcpkg-${TARGET_SYSTEM_PROCESSOR}-linux"
ARG ENGINE_REPO_SOURCE
ARG ENGINE_DIR

COPY $ENGINE_REPO_SOURCE $ENGINE_DIR

RUN --mount=type=cache,target=$CCACHE_PATH,from=engine-dependencies,id="$CCACHE-engine-community" \
    --mount=type=cache,target=$VCPKG_ROOT/buildtrees,sharing=locked,from=engine-dependencies \
    --mount=type=cache,target=$VCPKG_ROOT/downloads,sharing=locked,from=engine-dependencies \
    --mount=type=cache,target=$VCPKG_ROOT/installed,sharing=locked,from=engine-dependencies \
    --mount=type=cache,target=$VCPKG_ROOT/packages,sharing=locked,from=engine-dependencies \
    --mount=type=cache,target=/root/.cache/vcpkg,sharing=locked,from=engine-dependencies \
    --mount=type=cache,target=$ENGINE_BUILD_PATH,sharing=locked,from=engine-dependencies \
    source scl_source enable gcc-toolset-13 \
    && if [ -n "$NJOBS" ]; then export CMAKE_BUILD_PARALLEL_LEVEL="$NJOBS"; fi \
    && ccache --zero-stats \
    && cmake --workflow --preset ${ENGINE_PRESET}  \
    && ccache --show-stats -v -v


# ------------------------------------------------------------------------------
# Image with the built engine at /engine-package
FROM engine-build AS engine-package

ARG TARGETARCH
ARG PACKAGE_NAME="getml-community-$GETML_VERSION-$TARGETARCH-linux"
ARG ENGINE_PACKAGE_TGZ="engine-$GETML_VERSION-Linux-$TARGET_SYSTEM_PROCESSOR.tar.gz"

RUN --mount=type=cache,target=$ENGINE_BUILD_PATH,sharing=locked,from=engine-dependencies \
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
