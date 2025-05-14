// Copyright 2025 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"getML/install"
	"os"
	"path"
)

// Changes to the path containing the resources,
// such as config.json
func changeToResourceDir(packageName string) {

	execPath, err := os.Executable()

	if err != nil {
		return
	}

	execDir := path.Dir(execPath)

	if execDir == install.UsrLocalBin {
		os.Chdir(install.GetMainDir(install.UsrLocal, packageName))
		return
	}

	os.Chdir(execDir)
}
