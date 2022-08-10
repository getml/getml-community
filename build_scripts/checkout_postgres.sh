function checkout_postgres() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/postgres/postgres.git
    cd postgres || exit 1
    git checkout REL_14_1 || exit 1
}
