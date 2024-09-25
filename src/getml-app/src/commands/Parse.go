// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package commands

import (
	"getML/config"
	"getML/install"
	"os"
	"path/filepath"
)

// Parse parses the command line arguments
func Parse(packageName string) (*config.CommandLine, config.Config) {

	// TODO: ParseHomeDir in DefaultCommandLine
	commandLine := config.DefaultCommandLine()

	commandLine.ForceInstall = !install.FileExists(
		install.GetConfigPath(commandLine.HomeDir, packageName))

	conf := loadConfig(commandLine.ForceInstall, commandLine.HomeDir, packageName)

	if !filepath.IsAbs(conf.ProjectDirectory) {
		conf.ProjectDirectory = filepath.Join(
			install.GetBinDir(commandLine.HomeDir, packageName), "..", conf.ProjectDirectory)
	}

	runCmd := makeRunCommand(packageName, &conf, commandLine)

	installCmd := makeInstallCommand(commandLine)

	stopCmd := makeStopCommand(&conf)

	uninstallCmd := makeUninstallCommand(commandLine)

	if len(os.Args) == 1 {
		runCmd.Parse(os.Args[1:])
		return commandLine, conf
	}

	switch os.Args[1] {

	case "install":
		commandLine.ForceInstall = true
		commandLine.InstallOnly = true
		installCmd.Parse(os.Args[2:])

	case "run":
		runCmd.Parse(os.Args[2:])

	case "stop":
		commandLine.Stop = true
		stopCmd.Parse(os.Args[2:])

	case "uninstall":
		commandLine.Uninstall = true
		uninstallCmd.Parse(os.Args[2:])

	case "version":
		commandLine.ShowVersionOnly = true

	default:
		isFlag := printHelpMenu(packageName, runCmd)

		if isFlag {
			return commandLine, conf
		}

		return nil, conf
	}

	return commandLine, conf

}
