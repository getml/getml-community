function relink_dylibs_in_executable() {

    cd $GETML_PACKAGE_FOLDER/bin || exit 1
    
    install_name_tool -change @rpath/$TARGET_LIBXGBOOST $FRAMEWORKS_PATH/$TARGET_LIBXGBOOST $GETML_VERSION-64bit || exit 1
    install_name_tool -change @rpath/$TARGET_LIBMARIADB $FRAMEWORKS_PATH/$TARGET_LIBMARIADB $GETML_VERSION-64bit || exit 1
    install_name_tool -change $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBODBC $FRAMEWORKS_PATH/$TARGET_LIBODBC $GETML_VERSION-64bit || exit 1
    # install_name_tool -change libgoutils.dylib $FRAMEWORKS_PATH/libgoutils.dylib $GETML_VERSION-64bit || exit 1 
    install_name_tool -change /usr/lib/$TARGET_LIBCPP $FRAMEWORKS_PATH/$TARGET_LIBCPP $GETML_VERSION-64bit || exit 1

 }
