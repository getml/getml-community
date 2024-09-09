// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package commands

import (
	"flag"
	"fmt"
	"os"
)

func printHelpMenu(version string, runCmd *flag.FlagSet) bool {

	isFlag := (len(os.Args[1]) > 0 && os.Args[1][0] == '-')

	if !isFlag {
		fmt.Println("usage: ./getML <command> [<args>] or ./getML [<args>].")
		fmt.Println("")
		fmt.Println("Possible commands are: ")
		fmt.Println(` run        Runs getML. Type "./getML -h" or "./getML run -h" to display the arguments. "run" is executed by default.`)
		fmt.Println(` install    Installs getML.`)
		fmt.Println(` stop       Stops a running instance of getML. Type "./getML stop -h" to display the arguments.`)
		fmt.Println(` uninstall  Uninstalls ` + version + `.`)
		fmt.Println(` version    Prints the version (` + version + `).`)
		fmt.Println("")
	}

	runCmd.Parse(os.Args[1:])

	return isFlag
}
