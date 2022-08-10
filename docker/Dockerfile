# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

FROM ubuntu:20.04

RUN useradd getml

WORKDIR /home/getml

RUN apt-get upgrade
RUN apt-get update
RUN apt-get install -y git python3-pip wget
RUN apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

WORKDIR /home/getml 
RUN pip3 install "matplotlib>=3.4,<4.0" "jupyterlab>=3.2,<4.0"

# Uncomment the following lines if you want pyspark to
# run in your container.

# RUN pip3 install "pyspark>=3.2,<4.0"
# RUN apt update
# ENV DEBIAN_FRONTEND=noninteractive
# RUN apt -y install tzdata
# RUN apt install -y openjdk-8-jdk

ARG VERSION_NUMBER
ARG GETML_VERSION
ARG FORCE_RELOAD

WORKDIR /home/getml

RUN wget https://static.getml.com/download/$VERSION_NUMBER/$GETML_VERSION.tar.gz
RUN tar -xzvf $GETML_VERSION.tar.gz 

RUN wget https://static.getml.com/download/$VERSION_NUMBER/python.tar.gz
RUN tar -xzvf python.tar.gz 
WORKDIR python
RUN pip3 install .

WORKDIR /home/getml
RUN git clone https://github.com/getml/getml-community.git
WORKDIR /home/getml/getml-community/demo-notebooks
RUN git checkout $VERSION_NUMBER

WORKDIR /home/getml

# Windows tends to mess up newline characters,
# so we just create the launch_getml.sh in here.
RUN echo "#!/bin/bash\n\
LC_ALL='en_US.UTF-8'\n\
if [ -z \${1} ]; then export TOKEN=\$(openssl rand -hex 16); else export TOKEN=\$1; fi\n\
export URL_JUPYTER_NOTEBOOK=\$2\n\
cd /home/getml/storage\n\
mkdir -p notebooks\n\
cp -u /home/getml/getml-community/demo-notebooks/*.ipynb notebooks\n\
echo 'To access Jupyter Lab, please open a browser and point it to the following URL:'\n\
echo \$URL_JUPYTER_NOTEBOOK'/?token='\$TOKEN\n\
echo\n\
(nohup jupyter-lab --allow-root --ip='*' --NotebookApp.token=\$TOKEN & ) &> /dev/null \n\
cd /home/getml/"$GETML_VERSION"\n\
./getML --install --home-directory=/home/getml/storage --allow-remote-ips=true --token=\$TOKEN --proxy-url=\$URL_GETML_MONITOR/" >> launch_getml.sh

WORKDIR /home/getml
