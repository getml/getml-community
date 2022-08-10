function compile_mariadb() {
    cd $DEPENDENCY_DIR/mariadb-connector-c || exit 1
    mkdir -p build || exit 1
    cd build || exit 1
    cmake .. -DCMAKE_BUILD_TYPE=Release 
    make -j6
}

