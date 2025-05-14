// Copyright 2025 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package config

// CommandLine contains all the configurations
// that can only be passed via the command line,
// but not via config.json.
type CommandLine struct {
	ForceInstall bool

	HomeDir string

	InstallOnly bool

	ShowVersionOnly bool

	Stop bool

	Uninstall bool
}

// DefaultCommandLine is a constructor
func DefaultCommandLine() *CommandLine {
	return &CommandLine{
		ForceInstall:    false,
		HomeDir:         parseHomeDirFlag(),
		InstallOnly:     false,
		ShowVersionOnly: false,
		Stop:            false,
		Uninstall:       false,
	}

}
