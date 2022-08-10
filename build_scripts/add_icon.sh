# Adds an icon to getML.exe (Windows only)
function add_icon() {
  cd $HOMEDIR/../src/rcedit/Default || exit 1
  ./rcedit.exe "$GETML_PACKAGE_FOLDER/getML.exe" --set-icon "$HOMEDIR/../src/getml-app/getml_icon_tile6_1024_4TR_icon.ico" || exit 1
}
