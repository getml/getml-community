# Copy all required shared libraries into the Framework folder and
# link the binaries against these files instead of the system-wide
# ones.
function handle_dylibs() {
    echo -e "\n * ${COLOR_GREEN}Handling shared libraries...${COLOR_RESET}\n"

    cd $HOMEDIR || exit 1

    # --------------------------------------------------------------------

    if [ ! -d $GETML_PACKAGE_FOLDER/Frameworks ]; then
        mkdir $GETML_PACKAGE_FOLDER/Frameworks || exit 1
    fi
    
    copy_dylibs

    # --------------------------------------------------------------------
    
    relink_dylibs_in_executable
    
    relink_dylibs_in_odbc
    relink_dylibs_in_ssl
    relink_dylibs_in_xgboost

    # --------------------------------------------------------------------
    
     cd $HOMEDIR || exit 1
 }
