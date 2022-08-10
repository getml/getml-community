# This function build the code of the monitor located in the submodule
# in dependencies. Beware: it uses the code as is and does not pull a
# fresh copy.
function build_monitor() {
    echo -e "\n * ${COLOR_GREEN}Compiling the getML monitor...${COLOR_RESET}\n"

    export GOPATH=$HOMEDIR/../src/monitor || exit 1

    cd $GOPATH/src || exit 1

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        GOOS=linux go install || exit 1

    elif [[ "$OSTYPE" == "darwin"* ]]; then

        CC=clang CXX=clang++ GOOS=darwin go install || exit 1
        cd $HOMEDIR/../src/monitor || exit 1
        chmod +x ./bin/monitor || exit 1

    elif [[ "$OSTYPE" == "msys"* ]]; then

        GOOS=windows go install || exit 1
        cd $HOMEDIR/../src/monitor || exit 1
        chmod +x ./bin/monitor || exit 1

    fi

    cd $HOMEDIR/../src/monitor || exit 1

    if [ ! -d $GETML_PACKAGE_FOLDER/bin ];then
        mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1
    fi

    mv ./bin/monitor $GETML_PACKAGE_FOLDER/bin/getml-monitor || exit 1

    copy_templates

}
