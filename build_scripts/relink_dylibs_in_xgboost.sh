function relink_dylibs_in_xgboost() {

    cd $GETML_PACKAGE_FOLDER/Frameworks || exit 1
    
    install_name_tool -change /usr/lib/$TARGET_LIBCPP $FRAMEWORKS_PATH/$TARGET_LIBCPP $TARGET_LIBXGBOOST || exit 1

 }
