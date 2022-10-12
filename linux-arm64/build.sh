#! /bin/bash

 # --------------------------------------------------------------------

 export OPERATING_SYSTEM="linux"
 export FOLDER="linux-arm64"

 source ../build_scripts/import_all_functions.sh || exit 1
 source ../proxy_url.sh || export PROXY_URL=localhost
 echo "The proxy URL is set to "$PROXY_URL

 # Don't trust the version.sh script to detect the right version. The
 # docker build can be done on both Linux and macOS but the binary is
 # only intended to be used on Linux.
 export GETML_VERSION=$(echo $GETML_VERSION | sed -e 's/macos/linux/g')

 # Bucket to upload the data to.
 S3_BUCKET="dummy-testerella"

 # Get user name
 USER_NAME=$USER
 HOMEDIR=$PWD
 export DOCKER_IMAGE_NAME=almalinux8

 # Folder the results of the compilation will be stored in.
 GETML_BUILD_FOLDER="$HOMEDIR/build"

 # Folder that contains goutils
 GETML_GOUTILS_FOLDER="$HOMEDIR/../src/goutils"

 # Folder the final package will be located in.
 GETML_PACKAGE_FOLDER="$GETML_BUILD_FOLDER/$GETML_VERSION"

 # The package to download when the engine code is not present.
 export PRE_BUILD_PACKAGE="getml-1.0.0-linux-preview2"

 # The package to download when the engine code is not present.
 export PRE_BUILD_PACKAGE="getml-1.0.0-linux-preview2"

 # --------------------------------------------------------------------

 export CMAKE_BINARY_DIR="/home/getml/storage/$FOLDER/build/"
 export CODE_DIR="/home/getml/storage/src/engine"

 export DEPENDENCY_DIR="/home/getml/storage/src/dependencies" 
 export ARROW_DIR="$DEPENDENCY_DIR/arrow"
 export MARIADB_DIR="$DEPENDENCY_DIR/mariadb-connector-c"
 export POCO_DIR="$DEPENDENCY_DIR/poco"
 export POSTGRES_DIR="$DEPENDENCY_DIR/postgres"
 export UNIX_ODBC_DIR="$DEPENDENCY_DIR/unixODBC-2.3.9"
 export XGBOOST_DIR="$DEPENDENCY_DIR/xgboost"

 export GOUTILS_DIR=$GETML_GOUTILS_FOLDER

 # --------------------------------------------------------------------

 # Whether you want to activate the 
 # assertions, possible values are ON or OFF. 
 # Should always be OFF for releases.
 export DEBUG_MODE=OFF 

 # Whether you want to activate gperftools
 # profiling, possible values are ON or OFF. 
 # Should always be OFF for releases.
 export PROFILING_MODE=OFF

 export GCC_TOOLSET_DIR=/opt/rh/gcc-toolset-10/root/usr
 export CC=$GCC_TOOLSET_DIR/bin/gcc
 export CXX=$GCC_TOOLSET_DIR/bin/g++

 # Program the compilation will be done with.
 export CMAKE_COMMAND="cmake3 . -B"

 # Settings specific for the compilations of the engine. (XGBoost and
 # Poco will be compiled by Docker in the `Dockerfile`).
 export CMAKE_OPTIONS_GETML="\
     -DCMAKE_C_COMPILER=$CC \
     -DCMAKE_CXX_COMPILER=$CXX \
     -DGETML_VERSION=$GETML_VERSION \
     -DCMAKE_EXPORT_COMPILE_COMMANDS=ON"

 export CMAKE_OPTIONS_POCO="\
     -DCMAKE_C_COMPILER=$CC \
     -DCMAKE_CXX_COMPILER=$CXX \
     -DBUILD_SHARED_LIBS=NO"

 # --------------------------------------------------------------------

 source ../build_scripts/make_flags.sh

 # --------------------------------------------------------------------
