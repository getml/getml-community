#!/bin/bash

# Get user name
export USER_NAME=$USER

# Define docker image name
export DOCKER_IMAGE_NAME=centos7

# Define home directory
if [[ "$OSTYPE" == "linux-gnu" ]]; then
export HOMEDIR=/home/$USER_NAME
elif [[ "$OSTYPE" == "darwin"* ]]; then
export HOMEDIR=/Users/$USER_NAME
fi

# Rebuild docker container, if necessary.
sudo docker build --tag=centos7 .

# Start docker container and execute script
if [[ "$OSTYPE" == "linux-gnu" ]]; then
sudo docker run -it --rm -v "${HOMEDIR}":"/home/autosql/homedir/" ${DOCKER_IMAGE_NAME} bash build.sh -j6
else
sudo docker run -it --rm -v "${HOMEDIR}":"/home/autosql/homedir/" ${DOCKER_IMAGE_NAME} bash build.sh -j1
fi

# Make sure main user owns all files
sudo chown -R $USER_NAME $HOMEDIR/relboost-engine/$AUTOSQL_VERSION/*
