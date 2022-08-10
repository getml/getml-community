function checkout_mariadb() {
    cd $DEPENDENCY_DIR || exit 1
    git clone https://github.com/mariadb-corporation/mariadb-connector-c.git 
    cd mariadb-connector-c || exit 1
    git checkout v3.2.5 || exit 1
}
