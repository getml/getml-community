// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package main

import (
	"os"
	"path"
)

// The workingDirectory needs to be identical to the
// path of the executable.
func changeToExecutableDir() {

	execPath, err := os.Executable()

	if err != nil {
		return
	}

	execDir := path.Dir(execPath)

	os.Chdir(execDir)
}
