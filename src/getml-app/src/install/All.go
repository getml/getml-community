// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package install

// All installs everything.
func All(homeDir string, version string) {

	err := copyResources(UsrLocal, version)

	if err != nil {
		println("Could not install into '" + UsrLocal + "': " + err.Error())
		println("Global installation failed, most likely due to " +
			"missing root rights. Trying local installation instead.")
	} else {
		copyFile("getML", "", UsrLocalBin, true)
		return
	}

	err = copyResources(homeDir, version)

	if err != nil {
		println("Could not install into '" + homeDir + "': " + err.Error())
	}

}
