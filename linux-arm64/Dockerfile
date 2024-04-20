FROM quay.io/pypa/manylinux_2_28_aarch64

RUN useradd getml

RUN dnf -y update
RUN dnf -y install git
RUN dnf -y install make
RUN dnf -y install cmake
RUN dnf -y install ninja-build
RUN dnf -y install wget
RUN dnf -y install unzip
RUN dnf -y install zip
RUN dnf -y install gcc-toolset-13-gcc-c++

# Install node.js and npm
RUN wget https://nodejs.org/dist/v16.16.0/node-v16.16.0-linux-arm64.tar.xz
RUN tar -C /usr/local -xJvf node-v16.16.0-linux-arm64.tar.xz
ENV PATH=$PATH:/usr/local/node-v16.16.0-linux-arm64/bin

# Required for building PostgreSQL
RUN dnf install -y bison readline-devel zlib-devel openssl-devel wget
RUN dnf groupinstall -y 'Development Tools'

# Required for profiling, but not an 
# actual dependency.
# RUN dnf install -y https://extras.getpagespeed.com/release-latest.rpm

WORKDIR /home/getml

# Setup go
RUN wget https://go.dev/dl/go1.18.3.linux-arm64.tar.gz  
RUN tar -C /usr/local -xzf go1.18.3.linux-arm64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

COPY version.sh /home/getml/version.sh

# Install ccache
RUN wget https://github.com/ccache/ccache/releases/download/v4.8.1/ccache-4.8.1.tar.xz
RUN tar -xJvf ccache-4.8.1.tar.xz
WORKDIR ccache-4.8.1
RUN cmake . 
RUN make install -j4
ENV CCACHE_DIR=/home/getml/storage/.ccache 
ENV CCACHE_TEMPDIR=/home/getml/storage/.ccache

COPY version.sh /home/getml/version.sh

