cd ../../
MONOREPO_DIR=$PWD
cd src/relboost-engine
cp $MONOREPO_DIR/linux/build/compile_commands.json .
sed -i 's#/home/getml/storage/monorepo#'$MONOREPO_DIR'#g' compile_commands.json

