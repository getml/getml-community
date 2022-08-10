#!/bin/bash

# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

# If you want to channel the notebook 
# through a proxy, you can edit the following line 
# to automatically redirect to the correct URL.
#
# NOTE: Do NOT add trailing slashes (/).
URL_JUPYTER_NOTEBOOK=http://localhost:8888

function open_browser() {

    if [[ "$OSTYPE" == "linux-gnu"* ]]; then

        sleep 5
        xdg-open $1

    elif [[ "$OSTYPE" == "darwin"* ]]; then

        sleep 5
        open $1

    elif [[ "$OSTYPE" == "msys"* ]]; then

        sleep 10
        start $1

    fi
}

source ./src/version.sh

export TAG=getml_${VERSION_NUMBER//./_}

echo 'Launching Docker container...'
echo

export TOKEN=$(openssl rand -hex 10)

# Interactive shells do not work on
# Windows, but users of other operating
# systems should still have them.
if [[ "$OSTYPE" == "msys"* ]]; then
    export TTY_OR_NOTHING=
else
    export TTY_OR_NOTHING=-it
fi

open_browser $URL_JUPYTER_NOTEBOOK/?token=$TOKEN & > /dev/null

docker run -v getml:"/home/getml/storage" -p 8888:8888 --rm $TTY_OR_NOTHING $TAG bash launch_getml.sh $TOKEN $URL_JUPYTER_NOTEBOOK
