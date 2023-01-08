# Copyright 2022 The SQLNet Company GmbH
#
# This file is licensed under the Elastic License 2.0 (ELv2).
# Refer to the LICENSE.txt file in the root of the repository
# for details.
#
export VERSION_NUMBER=1.3.1

export ARCH=$(uname -m)

if [[ $ARCH -eq "aarch64" ]]; then
    export GETML_ARCH="arm64";
elif [[ $ARCH -eq "arm64" ]]; then
    export GETML_ARCH="arm64";
else
    export GETML_ARCH="x64";
fi

export GETML_VERSION=getml-$VERSION_NUMBER-$GETML_ARCH-community-edition-linux
echo $GETML_VERSION
