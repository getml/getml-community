# Builds the python wheel package
function build_wheel() {
    echo -e "\n * ${COLOR_GREEN}Building the wheel package...${COLOR_RESET}\n"
    WHEEL_FILE_NAME=getml-$VERSION_NUMBER-py3-none-manylinux_2_17_$WHEEL_ARCH.whl

    rm -f *.whl || exit 1
    rm -rf build || exit 1
    python3.8 -m pip install -U build || exit 1
    python3.8 -m build . --wheel || exit 1

    auditwheel addtag dist/$WHEEL_FILE_NAME || exit 1
    cp dist/$WHEEL_FILE_NAME $GETML_BUILD_FOLDER || exit 1
}
