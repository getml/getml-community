function exec_getml() {
    echo -e "\n * ${COLOR_GREEN}Starting getML...${COLOR_RESET}\n"

    cd $GETML_PACKAGE_FOLDER || exit 1

    ./getML --install || exit 1
}
