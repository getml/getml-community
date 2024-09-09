// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"runtime"
	"strings"
)

func makeVersion() string {

	version := "getml-1.5.0-" + getArch() + "-community-edition-linux"

	if runtime.GOOS == "windows" {
		version = strings.Replace(version, "linux", "windows", -1)
	}

	if runtime.GOOS == "darwin" {
		version = strings.Replace(version, "linux", "macos", -1)
	}

	return version
}
