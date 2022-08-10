// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import (
	"os"
	"runtime"
)

// CopyResources copies the resources into a
// newly created hidden folder in HOME, if
// necessary.
func copyResources(homeDir string, version string) {

	// ------------------------------------------------------------------------

	allFiles := []string{
		"config.json",
		"environment.json",
		"bin",
		"tests"}

	// Make sure that we don't copy any
	// random folder called "templates"
	if !filesExists(allFiles) {
		return
	}

	// ------------------------------------------------------------------------

	println("Installing getML...")

	// ------------------------------------------------------------------------

	mainDir := GetMainDir(homeDir, version)

	os.MkdirAll(mainDir, os.ModePerm)

	// ------------------------------------------------------------------------

	copyFile("config.json", "", mainDir, true)

	copyFile("config.json", "defaultConfig.json", mainDir, false)

	copyFile("environment.json", "", mainDir, false)

	copyFile("bin", "", mainDir, true)

	copyFile("projects", "", mainDir, false)

	copyFile("tests", "", mainDir, true)

	copyFile("getML", "", mainDir, true)

	if runtime.GOOS == "linux" {
		copyFile("lib", "", mainDir, true)
		copyFile("shape-main.png", "", mainDir, false)
	}

	if runtime.GOOS == "darwin" {
		copyFile("Frameworks", "", mainDir, true)
		copyFile("getml-cli", "", mainDir, true)
	}

	// ------------------------------------------------------------------------

	println("Successfully installed getML.")

	// ------------------------------------------------------------------------
}
