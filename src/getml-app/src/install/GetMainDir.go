// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package install

import (
	"path/filepath"
	"runtime"
)

// GetMainDir returns the main directory
// to which we are copying our files.
func GetMainDir(targetDir string, version string) string {

	if runtime.GOOS == "windows" {
		return "."
	}

	if targetDir == UsrLocal {
		return filepath.Join(GetHomeDir(targetDir), "getML/"+version)
	}

	return filepath.Join(GetHomeDir(targetDir), ".getML/"+version)

}
