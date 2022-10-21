# Copies all the remaining files required to ship a built getML
# application in the appropriate folder.
function build_bundle() {
    echo -e "\n * ${COLOR_GREEN}Bundle the getML package...${COLOR_RESET}\n"

    cd $GETML_PACKAGE_FOLDER || exit 1

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        cp $HOMEDIR/../src/package-build-imports/shape-main.png . || exit 1
    fi

    cp $HOMEDIR/../LICENSE.txt . || exit 1
    cp $HOMEDIR/../src/package-build-imports/config.json . || exit 1
    cp $HOMEDIR/../src/package-build-imports/jwks.pub.json . || exit 1
    cp $HOMEDIR/../src/package-build-imports/defaultEnvironment.json environment.json || exit 1
    cp -r $HOMEDIR/../src/package-build-imports/tests . || exit 1

    copy_python

    # ----------------------------------------------------------------

    mkdir -p projects

    # ----------------------------------------------------------------

    cp $HOMEDIR/../build_scripts/version.sh . || exit 1

    # ----------------------------------------------------------------

    cd $GETML_BUILD_FOLDER
    rm -f $GETML_VERSION.tar.gz
    tar -czvf $GETML_VERSION.tar.gz $GETML_VERSION >/dev/null 2>&1
    make_checksum $GETML_VERSION.tar.gz linux 
    rm -f python-$GETML_VERSION.tar.gz
    tar -czvf python-$GETML_VERSION.tar.gz python >/dev/null 2>&1

    # ----------------------------------------------------------------

    rm -rf  $HOMEDIR/../src/python-api/getml/.getML/ || exit 1
    mkdir -p $HOMEDIR/../src/python-api/getml/.getML/ || exit 1 
    cp -r $GETML_VERSION $HOMEDIR/../src/python-api/getml/.getML/ || exit 1

    # ----------------------------------------------------------------
}
