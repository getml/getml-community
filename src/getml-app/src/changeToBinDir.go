// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"getML/install"
	"log"
	"os"
)

// First tries to change into $HOME/.getML/.../bin. If that fails,
// it goes for /usr/local/getML/.../bin
func changeToBinDir(homeDir string, version string) error {

	errHome := os.Chdir(install.GetBinDir(homeDir, version))

	if errHome != nil {
		errUsrLocal := os.Chdir(install.GetBinDir(install.UsrLocal, version))

		if errUsrLocal != nil {
			log.Println(errHome.Error())
			log.Println(errUsrLocal.Error())
			return errUsrLocal
		}

		return nil
	}

	return nil
}
