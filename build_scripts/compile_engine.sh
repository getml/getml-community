# --------------------------------------------------------------------------

function compile_engine_linux() {

    cd $HOMEDIR || exit 1
    eval $CMAKE_COMMAND$GETML_BUILD_FOLDER $CMAKE_OPTIONS_GETML || exit 1

    export NUM_THREADS=$(nproc --all)
    cd $GETML_BUILD_FOLDER || exit 1
    make -j$NUM_THREADS || exit 1

    echo $PWD


    mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1
    mkdir -p $GETML_PACKAGE_FOLDER/lib || exit 1


    echo "Copying to $GETML_PACKAGE_FOLDER..."
    cp bin/$GETML_VERSION-64bit $GETML_PACKAGE_FOLDER/bin/ || exit 1
    copy_libs

}

# --------------------------------------------------------------------------

function compile_engine_macos() {


    export CPATH=$CPATH_NEW

    cd $HOMEDIR || exit 1

    eval $CMAKE_COMMAND$GETML_BUILD_FOLDER $CMAKE_OPTIONS_GETML || exit 1

    cd $HOMEDIR/build || exit 1

    make -j6 || exit 1

    cd $HOMEDIR || exit 1


    if [ ! -d "$GETML_PACKAGE_FOLDER/bin" ];then
        mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1
    fi

    cp $GETML_BUILD_FOLDER/bin/$GETML_VERSION-64bit $GETML_PACKAGE_FOLDER/bin/ || exit 1


    handle_dylibs

}

# --------------------------------------------------------------------------

function compile_engine_windows() {

    cd $HOMEDIR || exit 1

    mkdir -p cmake_build || exit 1

    cmake -B cmake_build -G"Visual Studio 16 2019" -S . -DCMAKE_TOOLCHAIN_FILE=$HOMEDIR/vcpkg/scripts/buildsystems/vcpkg.cmake

    cd cmake_build

    MSBuild.exe $GETML_VERSION-64bit.sln -m -p:Configuration=Release || exit 1

    mkdir -p $GETML_PACKAGE_FOLDER/bin || exit 1

    cp $HOMEDIR/bin/Release/$GETML_VERSION-64bit $GETML_PACKAGE_FOLDER/bin || exit 1

    copy_dlls

}

# --------------------------------------------------------------------------

function compile_engine() {

    cd $HOMEDIR || exit 1

    echo $HOMEDIR

    if [[ -d "$HOMEDIR/../src/engine" ]]; then
        echo -e "\n * ${COLOR_GREEN}Compiling the getML engine...${COLOR_RESET}\n" 
    else
        echo -e "\n * ${COLOR_GREEN}Downloading the getML engine...${COLOR_RESET}\n"
        download_engine
        return 0
    fi 


    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        compile_engine_linux

    elif [[ "$OSTYPE" == "darwin"* ]]; then

        compile_engine_macos

    elif [[ "$OSTYPE" == "msys"* ]]; then

        compile_engine_windows

    fi
}

# --------------------------------------------------------------------------
