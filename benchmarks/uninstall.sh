#!/bin/bash

# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

source ./src/version.sh

export TAG=getml_${VERSION_NUMBER//./_}

echo "This will do the following: "
echo "1) Stop all containers using the volume 'getml'"
echo "2) Remove all containers using the volume 'getml'"
echo "3) Delete all images tagged "$TAG
echo "4) Delete the volume 'getml'"
echo
read -p "Do you want to continue [y/N]? " -n 1 -r
echo    
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    [[ "$0" = "$BASH_SOURCE" ]] && exit 1 || return 1
fi

export GETML_CONTAINERS=$(docker container ls -a --filter=volume=getml -q) 
export GETML_IMAGES=$(docker images --filter=reference=$TAG -q)

echo "Stopping all containers using the volume 'getml'..."
docker stop $GETML_CONTAINERS 
echo

echo "Removing all containers using the volume 'getml'..."
docker rm $GETML_CONTAINERS
echo

echo "Deleting all images tagged '"$TAG"'..."
docker image rm $GETML_IMAGES
echo

echo "Deleting the volume 'getml'..."
docker volume rm getml
echo

read -p "getML is now uninstalled. Press any key to continue." -n 1 -r
