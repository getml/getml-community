function relink_dylibs_in_libcpp() {

    cd $GETML_PACKAGE_FOLDER/Frameworks || exit 1
    
    install_name_tool -change @rpath/$TARGET_LIBCPPABI $FRAMEWORKS_PATH/$TARGET_LIBCPPABI $TARGET_LIBCPP || exit 1

 }
