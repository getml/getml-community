// Copyright 2022 The SQLNet Company GmbH
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
	"runtime"
	"strconv"
)

func main() {

	changeToExecutableDir()

	version := makeVersion()

	commandLine, conf := commands.Parse(version)

	if commandLine == nil {
		os.Exit(2)
	}

	if commandLine.ShowVersionOnly {
		println(version)
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

		if runtime.GOOS != "windows" {
			os.RemoveAll(install.GetMainDir(commandLine.HomeDir, version))
		}

		os.Exit(0)
	}

	if commandLine.ForceInstall {
		install.All(commandLine.HomeDir, version)
	}

	if commandLine.InstallOnly {
		os.Exit(0)
	}

	err := os.Chdir(install.GetBinDir(commandLine.HomeDir, version))

	if err != nil {
		log.Println(err.Error())
		return
	}

	printStartMessage(version)

	projects := data.NewProjects(&conf, version)

	mainHandler := tcp.NewMainHandler(version, conf.ToFlags(), projects)

	tcpServer := tcp.NewServer(mainHandler, projects)

	tcpServer.Launch()

}
