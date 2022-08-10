function zip_docker() {

    echo -e "\n * ${COLOR_GREEN}Zipping the Docker scripts...${COLOR_RESET}\n"

    cd $HOMEDIR/../docker || exit 1

    export ZIPFILE=getml-$VERSION_NUMBER-docker.zip

    zip $ZIPFILE * src/*

    mv $ZIPFILE $GETML_BUILD_FOLDER

    cd $GETML_BUILD_FOLDER

    make_checksum $ZIPFILE docker

}
