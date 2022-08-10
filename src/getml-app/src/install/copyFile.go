// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package install

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

func copyFile(file string, newFileName string, mainDir string, overwrite bool) {

	if newFileName == "" {
		newFileName = file
	}

	destination := filepath.Join(mainDir, newFileName)

	if destStat, err := os.Stat(destination); err != nil || overwrite {

		sourceStat, err := os.Stat(file)

		if err != nil {
			return
		}

		if sourceStat.IsDir() {
			copyDir(file, mainDir, overwrite)
			return
		}

		if overwrite {

			createdLater := (destStat != nil) && (sourceStat.ModTime().Before(destStat.ModTime()) ||
				sourceStat.ModTime().Equal(destStat.ModTime()))

			if createdLater {
				return
			}
		}

		input, err := ioutil.ReadFile(file)

		if err != nil {
			panic(err)
		}

		err = ioutil.WriteFile(destination, input, sourceStat.Mode())

		if err != nil {
			panic(err)
		}

	}

}
