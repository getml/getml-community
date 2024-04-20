function compile_arrow() {
    set -e
    cd $DEPENDENCY_DIR/arrow/cpp || exit 1

    cmake \
        -DARROW_CSV=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_BZ2=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZSTD=ON \
        -DARROW_WITH_BROTLI=ON \
        -G Ninja \
        -B release \
        -S . || exit 1

    export NUM_THREADS=$(nproc --all)
    cmake --build release -j$NUM_THREADS || exit 1
}
