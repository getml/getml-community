function compile_postgres() {
    cd $DEPENDENCY_DIR/postgres || exit 1
    mkdir -p build || exit 1   
    ./configure --prefix=$PWD/build
    make || exit 1
    make install || exit 1
}
