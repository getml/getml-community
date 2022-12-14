# Copies all dependencies of the engine
function copy_libs() {

# ----------------------------------------------------------------    

echo -e "\n * ${COLOR_GREEN}Copying the libraries...${COLOR_RESET}\n"

# ----------------------------------------------------------------    

cd $GETML_PACKAGE_FOLDER || exit 1
rm -rf lib || exit 1
mkdir lib || exit 1

# ----------------------------------------------------------------    
# Copy all dependencies not found in
# https://www.linux.co.cr/ldp/lfs/appendixa/glibc.html

mkdir -p lib || exit 1

cp /lib64/libssl.so.10 lib || exit 1
cp /lib64/libcrypto.so.10 lib || exit 1
cp $MARIADB_DIR/build/libmariadb/libmariadb.so.3 lib || exit 1
cp $UNIX_ODBC_DIR/build/lib/libodbc.so.2 lib || exit 1
cp $XGBOOST_DIR/lib/libxgboost.so lib || exit 1
cp /lib64/libz.so.1 lib || exit 1
cp /lib64/libzstd.so.1 lib || exit 1
cp /lib64/libstdc++.so.6 lib || exit 1
cp /lib64/libgomp.so.1 lib || exit 1
cp /lib64/libgcc_s.so.1 lib || exit 1
cp /lib64/libbz2.so.1 lib || exit 1
cp /lib64/libkrb5.so.3 lib || exit 1
cp /lib64/libcom_err.so.2 lib || exit 1
cp /lib64/libk5crypto.so.3 lib || exit 1
cp /lib64/libkrb5support.so.0 lib || exit 1
cp /lib64/libkeyutils.so.1 lib || exit 1
cp /lib64/libgssapi_krb5.so.2 lib || exit 1
cp /lib64/libselinux.so.1 lib || exit 1
cp /lib64/libpcre.so.1 lib || exit 1

# ----------------------------------------------------------------
# These dependencies are only needed for profiling

if [[ $PROFILING_MODE == "ON" ]]; then 
    echo "WARNING: Copying dependencies for profiling. Do not release this!"
    cp /lib64/libunwind.so.8 lib || exit 1
    cp /lib64/libprofiler.so.0 lib || exit 1
fi

# ----------------------------------------------------------------

}

