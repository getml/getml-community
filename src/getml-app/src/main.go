// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"getML/commands"
	"getML/data"
	"getML/install"
	"getML/tcp"
	"log"
	"os"
	"strconv"
)

// overwritten at compile time through ldflags
var version = "0.0.0"

func main() {

	packageName := makePackageName()

	changeToResourceDir(packageName)

	commandLine, conf := commands.Parse(packageName)

	if commandLine == nil {
		os.Exit(2)
	}

	if commandLine.ShowVersionOnly {
		println(packageName)
		os.Exit(0)
	}

	if commandLine.Stop {
		err := stopExistingProcess(conf.Monitor.TCPPort)

		if err != nil {
			println("No getml-monitor running on tcp-port " +
				strconv.Itoa(conf.Monitor.TCPPort) + ".")
		}

		os.Exit(0)
	}

	if commandLine.Uninstall {
		uninstall(commandLine.HomeDir, packageName)
		os.Exit(0)
	}

	if commandLine.ForceInstall {
		install.All(commandLine.HomeDir, packageName)
	}

	if commandLine.InstallOnly {
		os.Exit(0)
	}

	err := os.MkdirAll(conf.ProjectDirectory, 0750)

	if err != nil {
		log.Println(err.Error())
		return
	}

	printStartMessage(packageName)

	projects := data.NewProjects(&conf, packageName)

	mainHandler := tcp.NewMainHandler(version, conf.ToFlags(), projects)

	tcpServer := tcp.NewServer(mainHandler, projects)

	tcpServer.Launch()

}
