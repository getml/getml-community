function make_checksum() {

    # ----------------------------------------------------------------

    PACKAGE=$1
    OPERATING_SYSTEM=$2

    # ----------------------------------------------------------------

    if [[ "$OPERATING_SYSTEM" == "docker" ]]; then
        YAML=sha256-package-community-edition-$OPERATING_SYSTEM.yaml
    else 
        YAML=sha256-package-$ARCHITECTURE-community-edition-$OPERATING_SYSTEM.yaml
    fi

    rm -f $YAML

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        SHA256_HASH=$(sha256sum $PACKAGE | awk '{print $1}')

    elif [[ "$OSTYPE" == "msys"* ]]; then

        SHA256_HASH=$(sha256sum $PACKAGE | awk '{print $1}')

    else

        SHA256_HASH=$(shasum -a 256 $PACKAGE | awk '{print $1}')

    fi



    echo -e "version_number: '"$VERSION_NUMBER"'\nsha256_hash:\n    - "$OPERATING_SYSTEM": "$SHA256_HASH >> $YAML

    # ----------------------------------------------------------------
}
