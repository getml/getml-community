function relink_dylibs_in_ssl() {

    cd $GETML_PACKAGE_FOLDER/Frameworks || exit 1

    install_name_tool -change @rpath/$TARGET_LIBCRYPTO $FRAMEWORKS_PATH/$TARGET_LIBCRYPTO $TARGET_LIBSSL || exit 1

 }
