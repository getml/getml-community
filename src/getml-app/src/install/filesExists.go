// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

// filesExists takes a list of filenames and makes sure
// that they all exist.
func filesExists(filenames []string) bool {

	allFilesExist := true

	for _, name := range filenames {

		if !FileExists(name) {
			allFilesExist = false
			break
		}
	}

	return allFilesExist
}
