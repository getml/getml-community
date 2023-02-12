cd ../../
MONOREPO_DIR=$PWD
cd src/engine

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    cp $MONOREPO_DIR/linux-arm64/build/compile_commands.json .
    sed -i 's#/home/getml/storage/monorepo#'$MONOREPO_DIR'#g' compile_commands.json
elif [[ "$OSTYPE" == "darwin"* ]]; then
    cp $MONOREPO_DIR/macos/build/compile_commands.json .
else 
    echo "Not supported"
fi
