FROM almalinux:8

RUN useradd getml

RUN dnf -y update
RUN dnf -y install git
RUN dnf -y install make
RUN dnf -y install cmake
RUN dnf -y install wget
RUN dnf -y install unzip
RUN dnf -y install nodejs 
RUN dnf -y install zip
RUN dnf -y install gcc-toolset-10-gcc-c++
RUN dnf -y install python38 python38-devel python38-pip

# Required for building PostgreSQL
RUN dnf install -y bison readline-devel zlib-devel openssl-devel wget
RUN dnf groupinstall -y 'Development Tools'

# Required for profiling, but not an 
# actual dependency.
RUN dnf install -y https://extras.getpagespeed.com/release-latest.rpm
RUN dnf install -y gperftools

WORKDIR /home/getml

# Setup go
RUN wget https://dl.google.com/go/go1.17.linux-amd64.tar.gz
RUN tar -C /usr/local -xzf go1.17.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# Setup the uploader
COPY uploader /home/getml/uploader/
WORKDIR uploader
RUN pip3 install .

COPY version.sh /home/getml/version.sh
