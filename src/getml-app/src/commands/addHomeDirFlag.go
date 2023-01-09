// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package commands

import (
	"flag"
	"getML/config"
	"getML/install"
)

func addHomeDirFlag(cmd *flag.FlagSet, commandLine *config.CommandLine) {
	cmd.StringVar(
		&commandLine.HomeDir,
		"home-directory",
		install.GetHomeDir(""),
		"The directory which should be treated as the home directory by getML. "+
			"getML will create a hidden folder named '.getML' in said directory. "+
			"This is where the binaries are installed, if "+
			"the install process does not have root rights.")
}
