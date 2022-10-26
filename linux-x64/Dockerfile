FROM quay.io/pypa/manylinux2014_x86_64

RUN useradd getml

RUN yum -y update
RUN yum -y install git
RUN yum -y install make
RUN yum -y install cmake
RUN yum -y install wget
RUN yum -y install unzip
RUN yum -y install zip

# Required for building PostgreSQL
RUN yum install -y bison readline-devel zlib-devel openssl-devel wget 
RUN yum install -y flex

RUN yum -y install epel-release
RUN yum -y install libzstd-devel
RUN yum install -y bzip2-devel libffi-devel 

# Required for profiling, but not an 
# actual dependency.
RUN yum install -y https://extras.getpagespeed.com/release-latest.rpm
RUN yum install -y gperftools

WORKDIR /home/getml

# Install node.js and npm
RUN wget https://nodejs.org/dist/v16.16.0/node-v16.16.0-linux-x64.tar.xz
RUN tar -C /usr/local -xJvf node-v16.16.0-linux-x64.tar.xz
ENV PATH=$PATH:/usr/local/node-v16.16.0-linux-x64/bin

# Install Golang
RUN wget https://go.dev/dl/go1.18.3.linux-amd64.tar.gz  
RUN tar -C /usr/local -xzf go1.18.3.linux-amd64.tar.gz
ENV PATH=$PATH:/usr/local/go/bin:$GOPATH/bin

# Build and setup Python 3.8
WORKDIR /home/getml
RUN wget https://www.python.org/ftp/python/3.8.12/Python-3.8.12.tgz
RUN tar xvf Python-3.8.12.tgz
WORKDIR /home/getml/Python-3.8.12
RUN ./configure --enable-optimizations
RUN make altinstall

RUN python3.8 -m pip install wheel auditwheel 

WORKDIR /home/getml

# Setup the uploader
COPY uploader /home/getml/uploader/
WORKDIR uploader
RUN python3.8 -m pip  install .

WORKDIR /home/getml

COPY version.sh /home/getml/version.sh
