// Copyright 2024 Code17 GmbH
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
func GetConfigPath(homeDir string, packageName string) string {

	binDir := filepath.Join(GetMainDir(homeDir, packageName), "config.json")

	return binDir
}
