function checkout_arrow() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/apache/arrow.git 
    cd arrow || exit 1
    git checkout apache-arrow-7.0.0 || exit 1
}
