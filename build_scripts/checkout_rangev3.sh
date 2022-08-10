function checkout_rangev3() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/ericniebler/range-v3.git
    cd range-v3 || exit 1 
    git checkout 83783f578e0e6666d68a3bf17b0038a80e62530e || exit 1
}
