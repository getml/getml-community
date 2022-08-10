# This builds the entry point for macOS (the Swift App)
function build_macos_entry_point() {

    # ----------------------------------------------------------------

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        return 
    
    elif [[ "$OSTYPE" == "msys"* ]]; then

        return
    fi

    # ----------------------------------------------------------------

    echo -e "\n * ${COLOR_GREEN}Compiling the getml-app-macos (the Swift app)...${COLOR_RESET}\n"
    
    # ----------------------------------------------------------------

    cd $HOMEDIR/../src/getml-app-macos || exit 1

    # ----------------------------------------------------------------
    
    xcodebuild || return

    # ----------------------------------------------------------------

    cp -r ./build/Release/getML.app $GETML_PACKAGE_FOLDER || exit 1

    # ----------------------------------------------------------------

    # TODO for the CD pipeline: Automate notarization.
    # Here are instructions on how this can be done. 
    # https://developer.apple.com/documentation/security/notarizing_macos_software_before_distribution/customizing_the_notarization_workflow

}
