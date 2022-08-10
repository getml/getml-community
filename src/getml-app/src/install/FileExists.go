// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import "os"

// FileExists checks whether a file exists.
func FileExists(name string) bool {

	_, err := os.Stat(name)

	return !os.IsNotExist(err)
}
