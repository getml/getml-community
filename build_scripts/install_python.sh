# Installs the proper version of the Python API and copies its
# artifacts in the package folder.
function install_python() {

    echo -e "\n * ${COLOR_GREEN}Installing the getML Python API...${COLOR_RESET}\n"

    # ----------------------------------------------------------------

    cd $HOMEDIR/../src/python-api || exit 1

    # ----------------------------------------------------------------

    if [[ "$OSTYPE" == "msys"* ]]; then
        python setup.py install || exit 1
        return
    fi

    # ----------------------------------------------------------------

    # Check whether the user is using Anaconda or raw Python.
    if [ "$(which conda | wc -l)" -gt "0" ]; then
        # Anaconda
        python3 setup.py install || exit 1
    else
        # Raw Python
        pip3 install --user . || exit 1
    fi

}
