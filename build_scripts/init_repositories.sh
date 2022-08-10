# Sets up the environment to build the package in. Meant to be invoked
# after a fresh cloning of the repository.
function init_repositories() {

    echo -e "\n * ${COLOR_GREEN}Initializing git submodules...${COLOR_RESET}\n"

    cd $HOMEDIR || exit 1

    if [[ "$OSTYPE" == "darwin"* ]]; then

        checkout_engine

    fi

    install_dependencies

}
