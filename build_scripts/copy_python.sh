# Copies the python API into the build folder
function copy_python() {

    # ----------------------------------------------------------------

    cd $HOMEDIR/../src/python-api || exit 1

    # ----------------------------------------------------------------

    if [ -d $GETML_BUILD_FOLDER/python ]; then
        rm -rf $GETML_BUILD_FOLDER/python || exit 1
    fi
    mkdir -p $GETML_BUILD_FOLDER/python/getml || exit 1

    # ----------------------------------------------------------------

    cp -r ./getml/* $GETML_BUILD_FOLDER/python/getml || exit 1
    cp -r ./setup.py $GETML_BUILD_FOLDER/python || exit 1
    cp -r ./LICENSE $GETML_BUILD_FOLDER/python || exit 1

    # ----------------------------------------------------------------

}
