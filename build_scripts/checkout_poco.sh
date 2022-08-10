function checkout_poco() {
    cd $DEPENDENCY_DIR || exit 1
    git clone -b master https://github.com/pocoproject/poco.git
    cd poco || exit 1
    git checkout poco-1.11.1-release || exit 1
}
