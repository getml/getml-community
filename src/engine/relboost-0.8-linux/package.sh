#!/bin/bash

# Define version
export RELBOOST_VERSION=relboost-0.8-linux

# Go into right folder
cd ./homedir/relboost-engine/$RELBOOST_VERSION

# Create build directory
rm -rf build
mkdir build

# Run cmake
cmake3 -H. -Bbuild

# Build AutoSQL
bash build.sh $1

# Go into right folder
cd ./homedir/relboost-engine/$RELBOOST_VERSION

# Copy all dependencies not found in
# https://www.linux.co.cr/ldp/lfs/appendixa/glibc.html
mkdir -p lib
cp /lib64/libgomp.so.1 lib
cp /lib64/libcrypto.so.10 lib
cp /lib64/libssl.so.10 lib
cp /lib64/libstdc++.so.6 lib
cp /lib64/libgssapi_krb5.so.2 lib
cp /lib64/libkrb5.so.3 lib
cp /lib64/libcom_err.so.2 lib
cp /lib64/libk5crypto.so.3 lib
cp /lib64/libkrb5support.so.0 lib
cp /lib64/libkeyutils.so.1 lib
cp /lib64/libselinux.so.1 lib
cp /lib64/libpcre.so.1 lib
cp /lib64/libpqxx-4.0.so lib
cp /lib64/libpq.so.5 lib
cp /lib64/libldap_r-2.4.so.2 lib
cp /lib64/liblber-2.4.so.2 lib
cp /lib64/libsasl2.so.3 lib
cp /lib64/libssl3.so lib
cp /lib64/libsmime3.so lib
cp /lib64/libplds4.so lib
cp /lib64/libplc4.so lib
cp /lib64/libnspr4.so  lib
cp /lib64/libfreebl3.so  lib

# Create temporary folder
mkdir -p $RELBOOST_VERSION

# Copy all of the files needed for the engine
# into temporary folder
cp setup.sh $RELBOOST_VERSION
cp run $RELBOOST_VERSION
cp config.json $RELBOOST_VERSION
cp jwks.pub.json $RELBOOST_VERSION

# Copy all of the folders needed into temporary folder
cp -r ./bin/ $RELBOOST_VERSION
cp -r ./lib/ $RELBOOST_VERSION
cp -r ./Python/ $RELBOOST_VERSION
cp -r ./tests/ $RELBOOST_VERSION
cp -r ./templates/ $RELBOOST_VERSION

# Create tarball
tar -cvzf $RELBOOST_VERSION.tar.gz $RELBOOST_VERSION

# Remove temporary directory
rm -r $RELBOOST_VERSION
