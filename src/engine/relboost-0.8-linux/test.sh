#!/bin/bash

# Define version
export RELBOOST_VERSION=relboost-0.8-linux

# Build AutoSQL
sh build.sh

# Go into right folder
cd ./homedir/relboost-engine/$RELBOOST_VERSION/bin

# Run tests
./relboost-0.8-linux-64bit-autosql-test
./relboost-0.8-linux-64bit-database-test
./relboost-0.8-linux-64bit-predictors-test
./relboost-0.8-linux-64bit-relboost-test

# Go back
cd ..
