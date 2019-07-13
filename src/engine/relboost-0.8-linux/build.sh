#!/bin/bash

export RELBOOST_VERSION=relboost-0.8-linux

export CC=/opt/rh/devtoolset-8/root/usr/bin/gcc
export CXX=/opt/rh/devtoolset-8/root/usr/bin/g++

# Move to build directory
cd /home/autosql/homedir/relboost-engine/$RELBOOST_VERSION/build

# Compile
make
