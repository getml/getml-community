function compile_xgboost_linux() {
    cd $DEPENDENCY_DIR/xgboost || exit 1
    mkdir -p build || exit 1
    cd build
    cmake .. || exit 1
    export NUM_THREADS=$(nproc --all)
    make -j$NUM_THREADS || exit 1
}


function compile_xgboost_macos() {
    cd $DEPENDENCY_DIR/xgboost || exit 1
    XGBOOST_BUILD_FOLDER="build"
    mkdir -p $XGBOOST_BUILD_FOLDER || exit 1
    eval $CMAKE_COMMAND$XGBOOST_BUILD_FOLDER $CMAKE_OPTIONS_XGBOOST || exit 1
    cd $XGBOOST_BUILD_FOLDER || exit 1
    make -j6 || exit 1
}

function compile_xgboost_windows() {
    cd $XGBOOST_DIR || exit 1
    mkdir -p build
    cd build
    cmake .. -G"Visual Studio 15 2017 Win64"
    cmake --build . --config Release
}

function compile_xgboost() {

    if [[ "$OSTYPE" == "darwin"* ]]; then

        compile_xgboost_macos

    elif [[ "$OSTYPE" == "msys"* ]]; then

        compile_xgboost_windows

    else

        compile_xgboost_linux

    fi

}
