// Copyright 2025 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package install

import (
	"path/filepath"
)

// GetBinDir returns the bin directory
// from which we are launching the Engine
// and the Monitor.
func GetBinDir(homeDir string, packageName string) string {
	return filepath.Join(GetMainDir(homeDir, packageName), "bin")
}
