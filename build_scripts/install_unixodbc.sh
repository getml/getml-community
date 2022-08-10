
function install_unixodbc() {

    cd $HOMEDIR/../src || exit 1
    export UNIXODBC=unixODBC-2.3.9
    if [ -f $UNIXODBC.tar.gz ]; then
        echo "$UNIXODBC already exists."
    else 
        wget http://www.unixodbc.org/$UNIXODBC.tar.gz
        tar -xzvf $UNIXODBC.tar.gz
    fi
    cd $UNIXODBC || exit 1
    ./configure --enable-gui=no --prefix=$PWD || exit 1
    make || exit 1
    make install || exit 1
}
