# Get user name
export USER_NAME=$USER

# Define docker image name
export DOCKER_IMAGE_NAME=fedora:29

# Define home directory
if [[ "$OSTYPE" == "linux-gnu" ]]; then
export HOMEDIR=/home/$USER_NAME
elif [[ "$OSTYPE" == "darwin"* ]]; then
export HOMEDIR=/Users/$USER_NAME
fi

# Rebuild docker container, if necessary.
sudo docker build --tag=fedora29 .

# Start docker container and execute script
sudo docker run -it --rm -v "${HOMEDIR}":"/home/autosql/homedir/" ${DOCKER_IMAGE_NAME} bash
