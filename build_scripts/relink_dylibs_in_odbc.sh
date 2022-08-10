function relink_dylibs_in_odbc() {

    cd $GETML_PACKAGE_FOLDER/Frameworks || exit 1
    
    install_name_tool -change $LDTL_DIR/lib/$TARGET_LIBLDTL @rpath/$TARGET_LIBLDTL $TARGET_LIBODBC || exit 1


 }
