function checkout_simdjson() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/simdjson/simdjson.git
    cd simdjson || exit 1
    git checkout v3.1.7 || exit 1
}
