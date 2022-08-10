function checkout_xgboost() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/dmlc/xgboost
    cd xgboost || exit 1 
    git checkout tags/v1.5.1 || exit 1
    git submodule update --init --recursive || exit 1
}
