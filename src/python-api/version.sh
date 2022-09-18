export VERSION_NUMBER=1.4.0

export GETML_VERSION_BASE=getml-$VERSION_NUMBER-community-edition

if [[ "$OSTYPE" == "linux-gnu" ]]; then

    export GETML_VERSION=$GETML_VERSION_BASE-linux

elif [[ "$OSTYPE" == "darwin"* ]]; then

    export GETML_VERSION=$GETML_VERSION_BASE-macos

elif [[ "$OSTYPE" == "msys"* ]]; then

    export GETML_VERSION=$GETML_VERSION_BASE-windows

else

    echo -e "\033[1;31m unknown OS $OSTYPE\033[0m" && exit 1

fi

