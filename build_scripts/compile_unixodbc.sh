function compile_unixodbc() {
    cd $DEPENDENCY_DIR/unixODBC-2.3.9 || exit 1
    mkdir -p build || exit 1   
    ./configure --prefix=$PWD/build
    make || exit 1
    make install || exit 1
}
