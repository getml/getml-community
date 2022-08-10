// Copyright 2022 The SQLNet Company GmbH
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

func loadConfig(forceInstall bool, homeDir string, version string) config.Config {

	fname := "./config.json"

	if !forceInstall {
		fname = install.GetConfigPath(homeDir, version)
	}

	return config.Load(fname)

}
