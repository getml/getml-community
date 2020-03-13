#!/bin/bash

# Run within the docker image to test the getML installation.

# --------------------------------------------------------------------

# Start the getML suite.
cd /home/getml/build/
# The particular name of the mounted folder depends on the current
# version.
cd "$(ls | grep getml)"

# Discard the output since we will run Python in the same session too.
./getML &> /dev/null &

# Let's give it some time to start and then ask the user to
# log in.
sleep 2
echo "Please log into the getML monitor, which is served at https://localhost:7710.

Press any button to proceed"

## Ask the user whether to kill the existing processes.
read -n 1

# --------------------------------------------------------------------

# Install the latest version of the Python API.
cd /home/getml/python_api/

# Using pip in here is important since the mounted directory is
# read-only. (We do not want Docker to mess with our local Python
# stuff on macOS)
pip3 install .

# --------------------------------------------------------------------

# Since the PATH variable is not properly set yet, we just hard code
# the location of the program.
pytest getml
pytest tests

# --------------------------------------------------------------------

# Check if a custom folder was generated in the home folder.
if [ "$(ls -lA /root/ | grep getML | wc -l)" -lt "1" ];then
	echo "Error: Custom folder $HOME/.getML not properly generated."
else
	echo "Custom folder $HOME/.getML successfully generated."
fi
