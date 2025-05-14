// Copyright 2025 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import "os/user"

// GetHomeDir returns the default home directory,
// unless another directory has been passed
// explicitly.
func GetHomeDir(homeDir string) string {

	if homeDir != "" {
		return homeDir
	}

	usr, err := user.Current()

	if err != nil {
		panic(err)
	}

	return usr.HomeDir
}
