// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package install

// All installs everything.
func All(homeDir string, version string) {

	installDir, err := copyResources(UsrLocal, version)

	if err != nil {
		println("Could not install into '" + UsrLocal + "': " + err.Error())
		println("Global installation failed, most likely due to " +
			"missing root rights. Trying local installation instead.")
	} else {
		copyFile("getML", "", UsrLocalBin, true)
		println("Installation successful. The getML Engine was installed into '" + installDir + "' and the cli was installed into '" + UsrLocalBin + "'.")
		return
	}

	installDir, err = copyResources(homeDir, version)

	if err != nil {
		println("Could not install into '" + homeDir + "': " + err.Error())
	} else {
		println("Installation successful. To be able to call 'getML' from anywhere, add the following to PATH:")
		println(installDir)
	}

}
