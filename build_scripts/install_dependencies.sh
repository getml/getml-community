function install_dependencies() {
    rm -rf $DEPENDENCY_DIR || exit 1
    mkdir -p $DEPENDENCY_DIR || exit 1

    checkout_arrow
    compile_arrow

    checkout_mariadb
    compile_mariadb

    checkout_poco
    compile_poco

    checkout_postgres
    compile_postgres

    checkout_reflectcpp 
    compile_reflectcpp

    download_unixodbc
    compile_unixodbc

    checkout_xgboost
    compile_xgboost
}

