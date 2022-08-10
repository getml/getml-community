# Copies the latest templates of the monitor.
function copy_templates() {
    echo -e "\n * ${COLOR_GREEN}Copying the templates...${COLOR_RESET}\n"

    # ----------------------------------------------------------------

    cd $HOMEDIR/../src/ || exit 1

    # ----------------------------------------------------------------

    if [ -d $GETML_PACKAGE_FOLDER/dist ]; then
        rm -rf $GETML_PACKAGE_FOLDER/frontend/dist/* || exit 1
    fi

    cp -r ./frontend/dist $GETML_PACKAGE_FOLDER || exit 1

    # ----------------------------------------------------------------

}
