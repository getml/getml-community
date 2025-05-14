// Copyright 2025 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package config

import (
	"path/filepath"
)

// Load loads the config.json.
func Load(fname string) Config {

	conf := Config{}

	fname, err := filepath.Abs(fname)

	if err != nil {
		panic(err.Error())
	}

	conf.GetFromFile(fname)

	return conf

}
