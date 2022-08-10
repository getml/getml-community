# Serves the frontend in development mode
function serve_frontend() {

    echo -e "\n * ${COLOR_GREEN}Serving the frontend in development mode...${COLOR_RESET}\n"

    cd $HOMEDIR/../src/frontend || exit 1

    npm install || exit 1

    npm run serve || exit 1
}
