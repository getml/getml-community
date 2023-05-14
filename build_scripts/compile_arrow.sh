function compile_arrow() {
    cd $DEPENDENCY_DIR/arrow/cpp || exit 1
    mkdir -p release || exit 1
    cd release || exit 1
    cmake -DARROW_CSV=ON -DARROW_PARQUET=ON -DARROW_WITH_BZ2=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZSTD=ON -DARROW_WITH_BROTLI=ON .. || exit 1
    export NUM_THREADS=$(nproc --all)
    make -j$NUM_THREADS || exit 1
}
