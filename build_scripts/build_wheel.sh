# Builds the python wheel package
function build_wheel() {
    echo -e "\n * ${COLOR_GREEN}Building the wheel package...${COLOR_RESET}\n"

    cd $HOMEDIR/../src/python-api || exit 1
    rm -f *.whl || exit 1
    python3 -m pip install wheel auditwheel || exit 1 # TODO: move into Dockerfile
    python3 -m pip wheel --no-deps . || exit 1

    # Necessary workaround because of a bug in auditwheel
    mv getml-$VERSION_NUMBER-py3-none-any.whl getml-$VERSION_NUMBER-cp38-manylinux_2_28-$WHEEL_ARCH.whl || exit 1

    rm -rf wheelhouse || exit 1
    auditwheel addtag dist/getml-$VERSION_NUMBER-cp38-manylinux_2_28-$WHEEL_ARCH.whl || exit 1
    cp wheelhouse/getml-*.whl $GETML_BUILD_FOLDER || exit 1
}
