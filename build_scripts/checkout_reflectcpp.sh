function checkout_reflectcpp() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/getml/reflect-cpp.git 
    cd reflect-cpp || exit 1
    git checkout 648d628de57211d2eda8fba07140b1d35503cfde || exit 1
}
