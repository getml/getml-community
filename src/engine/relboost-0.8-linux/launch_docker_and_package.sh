#!/bin/bash

export RELBOOST_VERSION=relboost-0.8-linux

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

# Compile Monitor
cd $HOMEDIR/relboost-monitor/Code
source gopath.sh
source install.sh

# Get Python code
cd $HOMEDIR/relboost-python-api
source install.sh

# Go back to relboost folder
cd $HOMEDIR/relboost-engine/$RELBOOST_VERSION

# Rebuild docker container, if necessary.
sudo docker build --tag=centos7 .

# Start docker container and execute script
sudo docker run -it --rm -v "${HOMEDIR}":"/home/autosql/homedir/" ${DOCKER_IMAGE_NAME} bash package.sh

# Make sure main user owns all files
sudo chown -R $USER_NAME $HOMEDIR/relboost-engine/$RELBOOST_VERSION/*
