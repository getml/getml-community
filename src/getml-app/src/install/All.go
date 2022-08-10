// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import (
	"runtime"
)

// All installs everything.
func All(homeDir string, version string) {

	if runtime.GOOS == "windows" {
		copyFile("config.json", "defaultConfig.json", "", false)
	}

	if runtime.GOOS == "darwin" {
		copyResources(homeDir, version)
	}

	if runtime.GOOS == "linux" {
		copyResources(homeDir, version)
		MakeDesktopEntry(homeDir, version)
	}

}
