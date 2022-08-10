# This builds the entry point for macOS (the Swift App)
function build_dmg() {

    # ----------------------------------------------------------------

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        return 
    
    elif [[ "$OSTYPE" == "msys"* ]]; then

        return
    fi

    # ----------------------------------------------------------------

    echo -e "\n * ${COLOR_GREEN}Building the DMG...${COLOR_RESET}\n"
    
    # ----------------------------------------------------------------

    cd $GETML_PACKAGE_FOLDER  || exit 1

    # ----------------------------------------------------------------
    
    if [ -d "diskimage_temp" ]; then rm -rf "diskimage_temp" || exit 1; fi  
    mkdir "diskimage_temp" || exit 1

    ln -s ~/Applications diskimage_temp/Applications
    cp -rf getML.app diskimage_temp

    # ----------------------------------------------------------------
     
    if [ -e "$GETML_VERSION-uncompressed.dmg" ]; then rm "$GETML_VERSION-uncompressed.dmg" || exit 1; fi  
    hdiutil create -size 500m -volname $GETML_VERSION -srcfolder "diskimage_temp" "$GETML_VERSION-uncompressed.dmg" -fs HFS+J || exit 1
 
    if [ -e "$GETML_VERSION.dmg" ]; then rm "$GETML_VERSION.dmg" || exit 1; fi  
    hdiutil convert "$GETML_VERSION-uncompressed.dmg" -format UDZO -o "$GETML_VERSION.dmg" || exit 1
   
    # ----------------------------------------------------------------

    rm -rf "diskimage_temp" || exit 1 
    rm "$GETML_VERSION-uncompressed.dmg" || exit 1

    # ----------------------------------------------------------------
    
    make_checksum $GETML_VERSION.dmg macos 
    
    # ----------------------------------------------------------------
    
}
