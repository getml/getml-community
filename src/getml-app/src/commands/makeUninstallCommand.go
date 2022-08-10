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
)

func makeUninstallCommand(commandLine *config.CommandLine) *flag.FlagSet {

	cmd := flag.NewFlagSet("uninstall", flag.ExitOnError)

	addHomeDirFlag(cmd, commandLine)

	return cmd

}
