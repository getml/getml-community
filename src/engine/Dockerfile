# syntax=docker/dockerfile:1.4

ARG ENGINE_REPO_SOURCE="."
ARG DEPENDENCY_DIR="/src/dependencies"
ARG ENGINE_DIR="/src/engine"
ARG CMAKE_BINARY_DIR="$ENGINE_DIR/build"

ARG ARROW_DIR="$DEPENDENCY_DIR/arrow" \
    MARIADB_DIR="$DEPENDENCY_DIR/mariadb-connector-c" \
    POCO_DIR="$DEPENDENCY_DIR/poco" \
    POSTGRES_DIR="$DEPENDENCY_DIR/postgres" \
    REFLECTCPP_DIR="$DEPENDENCY_DIR/reflect-cpp" \
    XGBOOST_DIR="$DEPENDENCY_DIR/xgboost"


FROM quay.io/pypa/manylinux_2_28_x86_64 as base-amd64

FROM quay.io/pypa/manylinux_2_28_aarch64 as base-arm64

FROM base-${TARGETARCH} as base

FROM base AS engine-base
ARG NJOBS TARGETARCH
RUN dnf groupinstall -y "Development Tools"
RUN dnf install -y \
    readline-devel \
    make \
    ninja-build \
    wget \
    openssl-devel \
    bison \
    zlib-devel \
    gcc-toolset-13-gcc-c++ \
    flex \
    https://extras.getpagespeed.com/release-latest.rpm


RUN git clone --depth 1 --branch v4.9.1 https://github.com/ccache/ccache.git /src/ccache
RUN source scl_source enable gcc-toolset-13 \
    && mkdir /src/ccache/build \
    && cd /src/ccache/build \
    && cmake -DCMAKE_BUILD_TYPE=Release .. \
    && make -j ${NJOBS:-$(nproc)} \
    && make install \
    && rm -r /src/ccache

WORKDIR ~


FROM engine-base AS engine-dependency-arrow-build
ARG NJOBS ARROW_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$ARROW_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export ARROW_VERSION=$(sed -n -e 's/\s*"arrow".*"\(.*\)".*/\1/p' ./dependencies.json) && \
    git clone --depth 1 --branch apache-arrow-$ARROW_VERSION https://github.com/apache/arrow.git $ARROW_DIR
WORKDIR $ARROW_DIR/cpp/build
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && cmake -G Ninja -DARROW_CSV=ON -DARROW_PARQUET=ON -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON -DARROW_USE_CCACHE=ON .. \
    && ninja \
    && cp -r $ARROW_DIR/cpp/build/ $ARROW_DIR/cpp/release


FROM scratch AS engine-dependency-arrow
ARG ARROW_DIR
COPY --from=engine-dependency-arrow-build $ARROW_DIR/cpp/src /arrow/cpp/src/
COPY --from=engine-dependency-arrow-build $ARROW_DIR/cpp/release/src /arrow/cpp/release/src/
COPY --from=engine-dependency-arrow-build $ARROW_DIR/cpp/release/release/*.a /arrow/cpp/release/release/


FROM engine-base AS engine-dependency-mariadb-build
ARG NJOBS MARIADB_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$MARIADB_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export MARIADB_VERSION=$(sed -n -e 's/\s*"mariadb".*"\(.*\)".*/\1/p' ./dependencies.json) && \
    git clone --depth 1 --branch v${MARIADB_VERSION} https://github.com/mariadb-corporation/mariadb-connector-c.git $MARIADB_DIR
WORKDIR $MARIADB_DIR/build
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && cmake -DCMAKE_BUILD_TYPE=Release -DWITH_OPENSSL=On -DWITH_EXTERNAL_ZLIB=On -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. \
    && make -j ${NJOBS:-$(nproc)} \
    && cp -r $MARIADB_DIR/build $MARIADB_DIR/release


FROM scratch AS engine-dependency-mariadb
ARG MARIADB_DIR
COPY --from=engine-dependency-mariadb-build $MARIADB_DIR/include /mariadb-connector-c/include/
COPY --from=engine-dependency-mariadb-build $MARIADB_DIR/release/include /mariadb-connector-c/release/include/
COPY --from=engine-dependency-mariadb-build $MARIADB_DIR/release/libmariadb/libmariadb.so.3 /mariadb-connector-c/release/libmariadb/


FROM scratch AS engine-dependency-mariadb-lib
ARG MARIADB_DIR
COPY --from=engine-dependency-mariadb-build $MARIADB_DIR/release/libmariadb/libmariadb.so.3 /lib/


FROM engine-base AS engine-dependency-poco-build
ARG NJOBS POCO_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$POCO_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export POCO_VERSION=$(sed -n -e 's/\s*"poco".*"\(.*\)".*/\1/p' ./dependencies.json) && \
    git clone --depth 1 --branch poco-${POCO_VERSION}-release https://github.com/pocoproject/poco.git $POCO_DIR
WORKDIR $POCO_DIR/build
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && cmake -DBUILD_SHARED_LIBS=NO -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. \
    && cmake --build . --config Release -j ${NJOBS:-$(nproc)} \
    && cp -r $POCO_DIR/build $POCO_DIR/release


FROM scratch AS engine-dependency-poco
ARG POCO_DIR
COPY --from=engine-dependency-poco-build $POCO_DIR/Crypto/include /poco/Crypto/include
COPY --from=engine-dependency-poco-build $POCO_DIR/JSON/include /poco/JSON/include
COPY --from=engine-dependency-poco-build $POCO_DIR/Net/include /poco/Net/include
COPY --from=engine-dependency-poco-build $POCO_DIR/NetSSL_OpenSSL/include /poco/NetSSL_OpenSSL/include
COPY --from=engine-dependency-poco-build $POCO_DIR/Foundation/include /poco/Foundation/include
COPY --from=engine-dependency-poco-build $POCO_DIR/release/build/lib/libPocoNetSSL.a \
                                         $POCO_DIR/release/build/lib/libPocoCrypto.a \
                                         $POCO_DIR/release/build/lib/libPocoNet.a \
                                         $POCO_DIR/release/build/lib/libPocoUtil.a \
                                         $POCO_DIR/release/build/lib/libPocoJSON.a \
                                         $POCO_DIR/release/build/lib/libPocoXML.a \
                                         $POCO_DIR/release/build/lib/libPocoFoundation.a \
                                         /poco/release/build/lib/


FROM engine-base AS engine-dependency-postgres-build
ARG NJOBS POSTGRES_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$POSTGRES_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export POSTGRES_VERSION=$(sed -n -e 's/\s*"postgres".*"\(.*\)\.\(.*\)".*/REL_\1_\2/p' ./dependencies.json) && \
    git clone --depth 1 --branch $POSTGRES_VERSION https://github.com/postgres/postgres.git $POSTGRES_DIR
WORKDIR $POSTGRES_DIR/build
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && ../configure --prefix=$PWD CC="ccache gcc" \
    && make -j ${NJOBS:-$(nproc)} \
    && make install \
    && cp -r $POSTGRES_DIR/build $POSTGRES_DIR/release


FROM scratch AS engine-dependency-postgres
ARG POSTGRES_DIR
COPY --from=engine-dependency-postgres-build $POSTGRES_DIR/release/include /postgres/release/include/
COPY --from=engine-dependency-postgres-build $POSTGRES_DIR/release/lib/libpq.a \
                                             $POSTGRES_DIR/release/lib/libpgport.a \
                                             $POSTGRES_DIR/release/lib/libpgcommon.a \
                                             /postgres/release/lib/


FROM engine-base AS engine-dependency-reflectcpp-build
ARG NJOBS REFLECTCPP_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$REFLECTCPP_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export REFLECTCPP_VERSION=$(sed -n -e 's/\s*"reflect-cpp".*"\(.*\)".*/\1/p' ./dependencies.json) && \
    git clone https://github.com/getml/reflect-cpp.git $REFLECTCPP_DIR
RUN git -C $REFLECTCPP_DIR checkout 648d628de57211d2eda8fba07140b1d35503cfde
WORKDIR $REFLECTCPP_DIR
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j ${NJOBS:-$(nproc)} \
    && cp -r $REFLECTCPP_DIR/build $REFLECTCPP_DIR/release


FROM scratch AS engine-dependency-reflectcpp
ARG REFLECTCPP_DIR
COPY --from=engine-dependency-reflectcpp-build $REFLECTCPP_DIR/include /reflect-cpp/include/
COPY --from=engine-dependency-reflectcpp-build $REFLECTCPP_DIR/build/libreflectcpp.a /reflect-cpp/build/


FROM engine-base AS engine-dependency-xgboost-build
ARG NJOBS XGBOOST_DIR ENGINE_REPO_SOURCE
ARG CCACHE_DIR="$XGBOOST_DIR/ccache"
COPY $ENGINE_REPO_SOURCE/dependencies.json ./dependencies.json
RUN export XGBOOST_VERSION=$(sed -n -e 's/\s*"xgboost".*"\(.*\)".*/\1/p' ./dependencies.json) && \
    git clone --recurse-submodules --remote-submodules --depth 1 --branch v${XGBOOST_VERSION} https://github.com/dmlc/xgboost $XGBOOST_DIR
WORKDIR $XGBOOST_DIR/build
RUN --mount=type=cache,target=$CCACHE_DIR \
    source scl_source enable gcc-toolset-13 \
    && cmake -DCMAKE_CXX_COMPILER_LAUNCHER=ccache .. \
    && make -j ${NJOBS:-$(nproc)} \
    && cp -r $XGBOOST_DIR/build $XGBOOST_DIR/release


FROM scratch AS engine-dependency-xgboost
ARG XGBOOST_DIR
COPY --from=engine-dependency-xgboost-build $XGBOOST_DIR/include /xgboost/include/
COPY --from=engine-dependency-xgboost-build $XGBOOST_DIR/rabit/include /xgboost/rabit/include/
COPY --from=engine-dependency-xgboost-build $XGBOOST_DIR/lib/libxgboost.so /xgboost/lib/


FROM scratch AS engine-dependency-xgboost-lib
ARG XGBOOST_DIR
COPY --from=engine-dependency-xgboost-build $XGBOOST_DIR/lib/libxgboost.so /lib/


FROM scratch AS engine-dependencies
ARG DEPENDENCY_DIR
COPY --from=engine-dependency-arrow . /dependencies
COPY --from=engine-dependency-mariadb . /dependencies
COPY --from=engine-dependency-poco . /dependencies
COPY --from=engine-dependency-postgres . /dependencies
COPY --from=engine-dependency-reflectcpp . /dependencies
COPY --from=engine-dependency-xgboost . /dependencies


FROM engine-base AS engine-build
ARG NJOBS DEPENDENCY_DIR ENGINE_REPO_SOURCE ENGINE_DIR
ARG PROFILING_MODE=OFF DEBUG_MODE=OFF
ARG ARROW_DIR MARIADB_DIR POSTGRES_DIR POCO_DIR REFLECTCPP_DIR XGBOOST_DIR CMAKE_BINARY_DIR

ARG CCACHE_DIR="$ENGINE_DIR/ccache" \
    ENGINE_SRC_SOURCE="$ENGINE_REPO_SOURCE/src" \
    ENGINE_INCLUDE_SOURCE="$ENGINE_REPO_SOURCE/include" \
    ENGINE_CMAKEFILE_SOURCE="$ENGINE_REPO_SOURCE/CMakeLists.txt" \
    ENGINE_DEPENDENCIES_FILE_SOURCE="$ENGINE_REPO_SOURCE/dependencies.json"

COPY $ENGINE_SRC_SOURCE $ENGINE_DIR/src
COPY $ENGINE_INCLUDE_SOURCE $ENGINE_DIR/include
COPY $ENGINE_CMAKEFILE_SOURCE $ENGINE_DIR/CMakeLists.txt
COPY $ENGINE_DEPENDENCIES_FILE_SOURCE $ENGINE_DIR/dependencies.json

WORKDIR $ENGINE_DIR/build

RUN --mount=type=cache,target=$CCACHE_DIR,sharing=locked \
    --mount=type=bind,target=$DEPENDENCY_DIR,from=engine-dependencies,source=dependencies \
    source scl_source enable gcc-toolset-13 \
    && cmake $ENGINE_DIR \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
    && make -j ${NJOBS:-$(nproc)}


FROM scratch AS package
ARG VERSION="unknown"
ARG ENGINE_DIR
ARG TARGETARCH
ARG TARGETOS
ARG PACKAGE_NAME="getml-$VERSION-$TARGETARCH-$TARGETOS"
COPY --from=engine-build $ENGINE_DIR/build/bin/engine /$PACKAGE_NAME/bin/engine
COPY --from=base \
    /lib64/libssl.so.1.1 \
    /lib64/libcrypto.so.1.1 \
    /lib64/libz.so.1 \
    /lib64/libzstd.so.1 \
    /lib64/libstdc++.so.6 \
    /lib64/libgomp.so.1 \
    /lib64/libgcc_s.so.1 /$PACKAGE_NAME/lib/
COPY --from=engine-dependency-mariadb-lib ./lib /$PACKAGE_NAME/lib/
COPY --from=engine-dependency-xgboost-lib ./lib /$PACKAGE_NAME/lib/
