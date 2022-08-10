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
	"os/user"
	"path/filepath"
)

// MakeDesktopEntry creates the desktop entry.
func MakeDesktopEntry(homeDir string, version string) {

	mainEntryPoint := filepath.Join(GetMainDir(homeDir, version), "getML")

	iconPath := filepath.Join(GetMainDir(homeDir, version), "shape-main.png")

	desktopEntry := []byte(`#! /usr/bin/env xdg-open

[Desktop Entry]
Name=getML
Exec=` + mainEntryPoint + `
Icon=` + iconPath + `
Terminal=false
Type=Application
Categories=Development`)

	usr, err := user.Current()

	if err != nil {
		return
	}

	applicationDir := filepath.Join(usr.HomeDir, ".local/share/applications/")

	err = os.MkdirAll(applicationDir, 0755)

	if err != nil {
		return
	}

	fname := filepath.Join(applicationDir, "getML.desktop")

	err = ioutil.WriteFile(fname, desktopEntry, 0644)

	if err != nil {
		return
	}
}
