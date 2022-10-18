package main

import (
	"getML/install"
	"os"
	"path"
	"runtime"
)

func uninstall(homeDir string, version string) {
	if runtime.GOOS != "windows" {
		os.RemoveAll(install.GetMainDir(homeDir, version))
		os.RemoveAll(install.GetMainDir(install.UsrLocal, version))
		os.Remove(path.Join(install.UsrLocalBin, "getML"))
	}
}
