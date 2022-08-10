function download_engine_linux(){
    URL="https://static.getml.com/download/$VERSION_NUMBER/$PRE_BUILD_PACKAGE.tar.gz"

    wget -nc $URL || exit 1

    tar xzf "$PRE_BUILD_PACKAGE.tar.gz" || exit 1

    cd $GETML_VERSION || exit 1

    echo "Copying to $GETML_PACKAGE_FOLDER..."
    mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1
    cp bin/$GETML_VERSION-64bit $GETML_PACKAGE_FOLDER/bin/ || exit 1
    cp -r lib $GETML_PACKAGE_FOLDER || exit 1
}

# Triggered when the engine folder is not found.
function download_engine() {

    cd $GETML_BUILD_FOLDER || exit 1

    mkdir -p "downloads" || exit 1

    cd "downloads" || exit 1

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        download_engine_linux

    elif [[ "$OSTYPE" == "msys"* ]]; then
        # TODO
        echo "TODO..." 
    else
        # TODO 
        echo "TODO..." 

    fi

}
