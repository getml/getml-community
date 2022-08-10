
# -------------------------------------------------------------------------------

function install_dependencies_linux() {
    rm -rf $DEPENDENCY_DIR || exit 1
    mkdir -p $DEPENDENCY_DIR || exit 1

    checkout_arrow
    compile_arrow

    # Only needed for profiling,
    # not a dependency in release mode.
    checkout_gperftools

    checkout_mariadb
    compile_mariadb

    checkout_poco
    compile_poco

    checkout_postgres
    compile_postgres

    download_unixodbc
    compile_unixodbc

    checkout_xgboost
    compile_xgboost
}

# -------------------------------------------------------------------------------

function install_dependencies_macos() {
    rm -rf $DEPENDENCY_DIR || exit 1
    mkdir -p $DEPENDENCY_DIR || exit 1

    cd $HOMEDIR/../src || exit 1

    brew install bison
    export PATH="/usr/local/opt/bison/bin:$PATH"
    export LDFLAGS="-L/usr/local/opt/bison/lib"

    brew install automake
    brew install cmake
    brew install npm
    brew install wget
    brew install llvm@13
    brew install pkg-config
    brew install libomp

    if [ ! -d vcpkg ]; then
        git clone https://github.com/microsoft/vcpkg.git 
        cd vcpkg 
        git checkout 2021.05.12
        ./bootstrap-vcpkg.sh
    fi

    cd $HOMEDIR/../src/vcpkg || exit 1
    ./vcpkg update || exit 1
    ./vcpkg install arrow[csv,parquet] --triplet x64-osx --disable-metrics || exit 1
    ./vcpkg install libiconv --triplet x64-osx-dynamic --recurse --disable-metrics || exit 1
    ./vcpkg install libmariadb[iconv,openssl,zlib] --triplet x64-osx-dynamic --recurse --disable-metrics || exit 1
    ./vcpkg install libpq[client] --triplet x64-osx --recurse --disable-metrics || exit 1
    ./vcpkg install poco[netssl] --triplet x64-osx --recurse --disable-metrics || exit 1
    ./vcpkg install unixodbc --triplet x64-osx-dynamic --recurse --disable-metrics || exit 1

    checkout_xgboost
    compile_xgboost

    checkout_rangev3
}

# -------------------------------------------------------------------------------

function install_dependencies_windows() {

    cd $HOMEDIR || exit 1
    git clone https://github.com/microsoft/vcpkg
    cd vcpkg || exit 1
    ./bootstrap-vcpkg.sh

    ./vcpkg.exe install poco[netssl] --triplet x64-windows --recurse --disable-metrics

    ./vcpkg.exe install poco[netssl] --triplet x64-windows --recurse --disable-metrics
    ./vcpkg.exe install libmariadb[iconv,openssl,zlib] --triplet x64-windows --recurse --disable-metrics
    ./vcpkg.exe install libpq[client] --triplet x64-windows --recurse --disable-metrics

    checkout_rcedit
    compile_rcedit

    checkout_poco
    checkout_xgboost

    compile_poco
    compile_xgboost

}

# -------------------------------------------------------------------------------

function install_dependencies() {

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        install_dependencies_linux

    elif [[ "$OSTYPE" == "darwin"* ]]; then

        install_dependencies_macos

    elif [[ "$OSTYPE" == "msys"* ]]; then

        install_dependencies_windows

    fi
}

# -------------------------------------------------------------------------------
