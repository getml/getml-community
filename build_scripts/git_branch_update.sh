# Retrieve the latest version of the develop branch of our own
# submodules. This function will overwrite all local changes when
# doing so!
function git_branch_update() {

    # ----------------------------------------------------------------

    if [ "$#" != "1" ]; then
        echo -e "\n * ${COLOR_RED}git_branch_update expects exactly one argument...${COLOR_RESET}\n"
        exit 1
    fi

    # ----------------------------------------------------------------

    echo -e "\n * ${COLOR_GREEN}Retrieving newest commits in branch $1 of submodules...${COLOR_RESET}\n"

    # ----------------------------------------------------------------

    echo -e "\n * ${COLOR_BLUE}Updating engine...${COLOR_RESET}\n"

    cd $HOMEDIR/../src/engine || exit 1
    git_handle_branch $1

    # ----------------------------------------------------------------

}
