# Define docker image name
export DOCKER_IMAGE_NAME=debian:latest

# Build docker image, if necessary.
sudo docker image build -t ${DOCKER_IMAGE_NAME} .

# Start docker container and execute script. It also maps the port
# 1709 within the container to the port 7709 at the Docker host (the
# OS on your laptop).
#
# Since these tests are intended to run within the `package-pipelines`
# git repository containing this repo as a submodule, we can work with
# relative paths while being still device independent.
sudo docker run --publish 7710:1710 -it --rm \
	 --mount type=bind,source="$(pwd)"/../../../../../linux/build/,destination=/home/getml/build/,readonly \
	 --mount type=bind,source="$(pwd)"/../../../../private-getml-python-api,destination=/home/getml/python_api/,readonly \
	 ${DOCKER_IMAGE_NAME} /home/getml/test_script.sh
