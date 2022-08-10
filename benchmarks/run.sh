#!/bin/bash

# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

source ./src/version.sh

export TAG=getml_${VERSION_NUMBER//./_}_benchmarks

echo 'Launching Docker container...'
echo

# Interactive shells do not work on
# Windows, but users of other operating
# systems should still have them.
if [[ "$OSTYPE" == "msys"* ]]; then
    export TTY_OR_NOTHING=
else
    export TTY_OR_NOTHING=-it
fi

docker run -v getml:"/home/getml/storage" -p 8888:8888 --rm $TTY_OR_NOTHING $TAG bash run_benchmarks.sh 
