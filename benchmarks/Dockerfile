# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#

FROM ubuntu:20.04

RUN useradd getml

WORKDIR /home/getml

RUN apt-get update
RUN apt-get install -y git wget gcc g++ --fix-missing
RUN apt-get install -y locales \
    && echo "en_US.UTF-8 UTF-8" > /etc/locale.gen \
    && locale-gen

RUN wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh
RUN bash Anaconda3-2022.05-Linux-x86_64.sh -b -p /home/getml/anaconda
ENV PATH="/home/getml/anaconda/bin:$PATH"

RUN pip install "tsfresh==0.18"
RUN pip install "featuretools==1.8.0" \
    "tsflex==0.2.3.7.6" "cesium==0.9.12" "tsfel==0.1.4"
RUN pip install "kats==0.1.0"

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

# We need to install these so the
# benchmark libraries work properly
RUN pip install numpy==1.21
RUN pip install ax-platform==0.2.3

WORKDIR /home/getml

COPY run_benchmarks.py .
COPY benchmarks ./benchmarks

# Windows tends to mess up newline characters,
# so we just create the launch_getml.sh in here.
RUN echo "#!/bin/bash\n\
LC_ALL='en_US.UTF-8'\n\
cd $GETML_VERSION\n\
nohup ./getML --install --home-directory=/home/getml/storage &\n\
cd /home/getml\n\
python3 run_benchmarks.py" >> run_benchmarks.sh

WORKDIR /home/getml
