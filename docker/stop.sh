#!/bin/bash

# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

source ./src/version.sh

export TAG=getml_${VERSION_NUMBER//./_}

echo "This will stop all containers using the volume 'getml'"
echo
read -p "Do you want to continue [y/N]? " -n 1 -r
echo    
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

export GETML_CONTAINERS=$(docker container ls -a --filter=volume=getml -q) 

echo "Stopping all containers using the volume 'getml'..."
docker stop $GETML_CONTAINERS 
echo

read -p "getML has now been stopped." -n 1 -r
