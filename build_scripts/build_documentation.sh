function build_documentation() {

    echo -e "\n * ${COLOR_GREEN}Building the documentation locally...${COLOR_RESET}\n"

    pip3 install sphinxcontrib.httpdomain sphinx.automodapi

    cd $HOMEDIR/../src/documentation || exit 1

    rm -rf html || exit 1
    rm -rf docs/api || exit 1
    sphinx-build -b html docs html || exit 1

}
