package main

import (
	"runtime"
	"strings"
)

func getArch() string {
	if strings.Contains(runtime.GOARCH, "arm") {
		return "arm64"
	}
	return "x64"
}
