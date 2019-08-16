#!/bin/bash

# Define version
export GETML_VERSION=getml-0.8-beta-macos-64bit

# Move to build directory
cd build

# Compile
make -j6

# Move back to main directory
cd ..

# Copy dynamic libraries.
# otool -L getml-0.8-beta-macos-64bit
cp /usr/local/opt/openssl/lib/libssl.1.0.0.dylib ./lib/	
cp /usr/local/opt/openssl/lib/libcrypto.1.0.0.dylib ./lib/
cp /usr/local/opt/gcc/lib/gcc/9/libstdc++.6.dylib ./lib/
cp /usr/local/opt/gcc/lib/gcc/9/libgomp.1.dylib ./lib/
cp /usr/local/lib/gcc/9/libgcc_s.1.dylib ./lib/

# Make sure I own the dynamic libraries.
sudo chown patrickurbanke ./lib/libcrypto.1.0.0.dylib
sudo chown patrickurbanke ./lib/libssl.1.0.0.dylib
sudo chown patrickurbanke ./lib/libstdc++.6.dylib
sudo chown patrickurbanke ./lib/libgomp.1.dylib
sudo chown patrickurbanke ./lib/libgcc_s.1.dylib

# Make sure I have can edit the dynamic libraries.
chmod +w ./lib/libcrypto.1.0.0.dylib
chmod +w ./lib/libssl.1.0.0.dylib
chmod +w ./lib/libstdc++.6.dylib
chmod +w ./lib/libgomp.1.dylib
chmod +w ./lib/libgcc_s.1.dylib

# Move to bin
cd bin

# Change paths.
install_name_tool -change /usr/local/opt/openssl/lib/libcrypto.1.0.0.dylib @executable_path/../lib/libcrypto.1.0.0.dylib $GETML_VERSION-64bit
install_name_tool -change /usr/local/opt/openssl/lib/libssl.1.0.0.dylib @executable_path/../lib/libssl.1.0.0.dylib $GETML_VERSION-64bit
install_name_tool -change /usr/local/opt/gcc/lib/gcc/9/libstdc++.6.dylib @executable_path/../lib/libstdc++.6.dylib $GETML_VERSION-64bit
install_name_tool -change /usr/local/opt/gcc/lib/gcc/9/libgomp.1.dylib @executable_path/../lib/libgomp.1.dylib $GETML_VERSION-64bit
install_name_tool -change /usr/local/lib/gcc/9/libgcc_s.1.dylib @executable_path/../lib/libgcc_s.1.dylib $GETML_VERSION-64bit

# Move back to main directory
cd ..

# Move to lib
cd lib

# Change paths for libgomp.1.dylib
install_name_tool -change /usr/local/lib/gcc/9/libgcc_s.1.dylib @executable_path/../lib/libgcc_s.1.dylib libgomp.1.dylib

# Change paths for libssl.1.0.0.dylib
install_name_tool -change /usr/local/Cellar/openssl/1.0.2p/lib/libcrypto.1.0.0.dylib @executable_path/../lib/libcrypto.1.0.0.dylib libssl.1.0.0.dylib

# Change paths for libstdc++.6.dylib
install_name_tool -change /usr/local/lib/gcc/9/libgcc_s.1.dylib @executable_path/../lib/libgcc_s.1.dylib libstdc++.6.dylib

# Move back to main directory
cd ..



