#!/bin/bash

# Define version
export RELBOOST_VERSION=relboost-0.8-linux

# Give rights to be executed
chmod +755 ./bin/$RELBOOST_VERSION-64bit
chmod +755 ./bin/autosql-monitor
chmod +755 run

# Print success message
echo "Setup successful. Type ./run to launch AutoSQL."
