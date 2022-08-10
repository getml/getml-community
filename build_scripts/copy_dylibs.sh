# Copy all required shared libraries into the Framework folder and
# link the binaries against these files instead of the system-wide
# ones.
function copy_dylibs() {

    # Use otool -L getml-... to verify the right ones are copied.
    echo "Root privileges are required to copy the dynamic shared libs."
    cp $XGBOOST_DIR/lib/$TARGET_LIBXGBOOST $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    
    cp $CLANG_DIR/lib/$TARGET_LIBCPP $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    cp $CLANG_DIR/lib/$TARGET_LIBCPPABI $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1

    cp $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBMARIADB $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    cp $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBODBC $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    cp $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBCRYPTO $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    cp $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBSSL $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    cp $VCPKG_DYNAMIC_DIR/lib/$TARGET_LIBZ $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
    
    sudo cp $LDTL_DIR/lib/$TARGET_LIBLDTL $GETML_PACKAGE_FOLDER/Frameworks/ || exit 1
   
    sudo chown $USER $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBLDTL || exit 1

    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBMARIADB || exit 1
    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBXGBOOST || exit 1
    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBODBC || exit 1
    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBCPP || exit 1
    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBCPPABI || exit 1
    chmod +w $GETML_PACKAGE_FOLDER/Frameworks/$TARGET_LIBLDTL || exit 1

 }
