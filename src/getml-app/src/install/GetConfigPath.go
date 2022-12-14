// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import (
	"path/filepath"
)

// GetConfigPath returns the where the config.json
// is supposed to be, if the software has been installed.
func GetConfigPath(homeDir string, version string) string {

	binDir := filepath.Join(GetMainDir(homeDir, version), "config.json")

	return binDir
}
