function compile_engine() {

    cd $HOMEDIR || exit 1

    echo $HOMEDIR

    echo -e "\n * ${COLOR_GREEN}Compiling the getML engine...${COLOR_RESET}\n" 

    cd $HOMEDIR || exit 1
    eval $CMAKE_COMMAND $GETML_BUILD_FOLDER $CMAKE_OPTIONS_GETML || exit 1

    export NUM_THREADS=$(nproc --all)
    cmake --build $GETML_BUILD_FOLDER -j$NUM_THREADS || exit 1

    echo $PWD


    mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1
    mkdir -p $GETML_PACKAGE_FOLDER/lib || exit 1

    echo "Copying to $GETML_PACKAGE_FOLDER..."
    cp $GETML_BUILD_FOLDER/bin/$GETML_VERSION-engine $GETML_PACKAGE_FOLDER/bin/ || exit 1
    copy_libs
}

