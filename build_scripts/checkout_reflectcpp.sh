function checkout_reflectcpp() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/getml/reflect-cpp.git 
    cd reflect-cpp || exit 1
    git checkout v0.9.0 || exit 1
}
