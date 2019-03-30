#!/bin/bash

# Build AutoSQL
sh build.sh

# Go to bin directory
cd bin

# Run tests
./relboost-0.8-ubuntu-64bit-test
