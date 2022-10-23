# Compile all sources and generate the final binaries.
function build_package() {

    echo -e "\n * ${COLOR_GREEN}Start packaging...${COLOR_RESET}\n"

    if [[ -d $GETML_BUILD_FOLDER ]]; then 
        rm -rf $GETML_BUILD_FOLDER || exit 1
    fi

    mkdir -p $GETML_BUILD_FOLDER || exit 1
    mkdir -p $GETML_PACKAGE_FOLDER || exit 1

    cp ../build_scripts/version.sh .

    build_main_entry_point || exit 1

    compile_engine || exit 1

    cd $HOMEDIR || exit 1

    build_bundle || exit 1

    build_wheel || exit 1
}
