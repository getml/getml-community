// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package install

import (
	"errors"
	"os"
)

// copyResources copies the resources into the
// targetDir, if necessary.
func copyResources(targetDir string, version string) (string, error) {

	allFiles := []string{
		"config.json",
		"environment.json",
		"bin",
		"tests"}

	// Make sure that we don't copy any
	// random folder called "templates"
	if !filesExists(allFiles) {
		return "", errors.New(
			"Could not find all necessary files, quitting installation.")
	}

	println("Installing getML...")

	mainDir := GetMainDir(targetDir, version)

	err := os.MkdirAll(mainDir, os.ModePerm)

	if err != nil {
		return "", err
	}

	err = copyFile("config.json", "", mainDir, true)

	if err != nil {
		return "", err
	}

	err = copyFile("config.json", "defaultConfig.json", mainDir, false)

	if err != nil {
		return "", err
	}

	err = copyFile("environment.json", "", mainDir, false)

	if err != nil {
		return "", err
	}

	err = copyFile("bin", "", mainDir, true)

	if err != nil {
		return "", err
	}

	err = copyFile("tests", "", mainDir, true)

	if err != nil {
		return "", err
	}

	err = copyFile("getML", "", mainDir, true)

	if err != nil {
		return "", err
	}

	err = copyFile("lib", "", mainDir, true)
	if err != nil {
		return "", err
	}
	
	err = copyFile("shape-main.png", "", mainDir, false)
	if err != nil {
		return "", err
	}

	println("Successfully installed getML into '" + mainDir + "'.")

	return mainDir, nil
}
