function checkout_yyjson() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/ibireme/yyjson.git
    cd yyjson || exit 1
    git checkout 0.6.0 || exit 1
}
