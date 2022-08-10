function download_unixodbc() {
    cd $DEPENDENCY_DIR || exit 1
    wget http://www.unixodbc.org/unixODBC-2.3.9.tar.gz || exit 1
    tar -xzvf unixODBC-2.3.9.tar.gz || exit 1
}
