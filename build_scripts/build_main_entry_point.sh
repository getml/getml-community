# This function build the code of the main entry point located in the
# submodule in dependencies. Beware: it uses the code as is and does
# not pull a fresh copy.
function build_main_entry_point() {
    echo -e "\n * ${COLOR_GREEN}Compiling the getml-app (the main entry point)...${COLOR_RESET}\n"

    export GOPATH=$HOMEDIR/../src/getml-app || exit 1

    cd $GOPATH/src || exit 1

    go install -buildvcs=false || exit 1

    cd $HOMEDIR/../src/getml-app || exit 1

    mv ./bin/getML $GETML_PACKAGE_FOLDER || exit 1

}
