function copy_dlls() {

    echo -e "\n * ${COLOR_GREEN}Copying the libraries...${COLOR_RESET}\n"

    cd $GETML_PACKAGE_FOLDER

    mkdir -p bin

    cp "$MSVC_LIB"/Microsoft.VC142.CRT/*.dll bin
    cp "$MSVC_LIB"/Microsoft.VC142.CXXAMP/*.dll bin
    cp "$MSVC_LIB"/Microsoft.VC142.OpenMP/*.dll bin

    cp $HOMEDIR/bin/Release/*.dll bin

    cp $XGBOOST_BIN/xgboost.dll bin

}
