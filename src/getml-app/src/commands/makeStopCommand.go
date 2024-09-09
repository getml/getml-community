// Copyright 2024 Code17 GmbH
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

func makeStopCommand(conf *config.Config) *flag.FlagSet {

	cmd := flag.NewFlagSet("stop", flag.ExitOnError)

	cmd.IntVar(
		&conf.Monitor.TCPPort,
		"tcp-port",
		conf.Monitor.TCPPort,
		"The TCP port of the getML instance you would like to stop.")

	return cmd

}
