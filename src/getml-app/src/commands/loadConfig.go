// Copyright 2025 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package commands

import (
	"getML/config"
	"getML/install"
)

func loadConfig(forceInstall bool, homeDir string, packageName string) config.Config {

	fname := "./config.json"

	if !forceInstall {
		fname = install.GetConfigPath(homeDir, packageName)
	}

	return config.Load(fname)

}
