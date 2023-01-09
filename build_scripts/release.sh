function release() {

    build_package || exit 1 

    zip_docker || exit 1 

    echo -e "\n * ${COLOR_GREEN}Uploading file to the Google Cloud...${COLOR_RESET}\n"

    cd $HOMEDIR/build

    cp $HOMEDIR/../getml-infra-service-key.json . || {
        echo "Could not find the service key required for the upload. Please ask a colleague." 
            exit 1
        }

    getml_uploader -f -r $VERSION_NUMBER sha256-package-community-edition-docker.yaml getml-$VERSION_NUMBER-community-edition-docker.zip sha256-package-community-edition-linux.yaml $GETML_VERSION.tar.gz 

    rm getml-infra-service-key.json
}
