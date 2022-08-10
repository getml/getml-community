function compile_poco() {
    cd $DEPENDENCY_DIR/poco || exit 1
    mkdir -p cmake-build || exit 1
    cd cmake-build || exit 
    cmake $CMAKE_OPTIONS_POCO .. || exit 1
    cmake --build . --config Release || exit 1
}
