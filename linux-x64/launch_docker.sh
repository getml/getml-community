#! /bin/bash

export DOCKER_IMAGE_NAME=almalinux8

docker run -it --rm --ulimit memlock=-1 \
    -v "$PWD/..":"/home/getml/storage" \
    -w "/home/getml/storage/linux" \
    ${DOCKER_IMAGE_NAME} bash 
