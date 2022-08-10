
function init_docker() {

    echo -e "\n * ${COLOR_GREEN}Building the Docker image...${COLOR_RESET}\n"

    cd $HOMEDIR || exit 1

    # ----------------------------------------------------------------

    macos_start_docker

    # ----------------------------------------------------------------

    cp ../build_scripts/version.sh . || exit 1

    docker build --tag=$DOCKER_IMAGE_NAME . || exit 1
}
