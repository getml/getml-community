function checkout_engine() {
    echo -e "\n * ${COLOR_GREEN}Initializing engine...${COLOR_RESET}\n"

    cd $HOMEDIR/../src/ || exit 1
    git clone git@github.com:getml/engine.git 

    cd $HOMEDIR/../src/engine || exit 1 
    git checkout develop || exit 1
    git pull 
}

