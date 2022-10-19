// Copyright 2022 The SQLNet Company GmbH
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

	version := "getml-1.3.0-" + getArch() + "-community-edition-linux"

	if runtime.GOOS == "windows" {
		version = strings.Replace(version, "linux", "windows", -1)
	}

	if runtime.GOOS == "darwin" {
		version = strings.Replace(version, "linux", "macos", -1)
	}

	return version
}
