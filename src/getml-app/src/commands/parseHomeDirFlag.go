// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package commands

import (
	"os"
	"strings"
)

func parseHomeDirFlag() string {

	isProperDirectory := func(homeDir string) bool {

		info, err := os.Stat(homeDir)

		if os.IsNotExist(err) {
			return false
		}

		if !info.IsDir() {
			return false
		}

		return true

	}

	for i := 0; i < len(os.Args); i++ {

		arg := os.Args[i]

		if !strings.Contains(arg, "-home-directory") {
			continue
		}

		if strings.Contains(arg, "-home-directory=") {
			homeDir := strings.Split(arg, "-home-directory=")[1]
			if isProperDirectory(homeDir) {
				return homeDir
			}
			return ""
		}

		if i+1 == len(os.Args) {
			return ""
		}

		homeDir := os.Args[i+1]

		if isProperDirectory(homeDir) {
			return homeDir
		}

		return ""
	}

	return ""
}
