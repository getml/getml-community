#!/bin/bash

# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

source ./src/version.sh

export TAG=getml_${VERSION_NUMBER//./_}

echo "Building the docker container..."
docker build --build-arg GETML_VERSION=$GETML_VERSION --build-arg VERSION_NUMBER=$VERSION_NUMBER --build-arg FORCE_RELOAD=0 --tag=$TAG . || exit 1
echo 

echo "Creating the volume 'getml'..."
docker volume create getml || exit 1
echo

read -p "Setup successful. You can now launch getML using run.sh. Press any key to continue." -n 1 -r
