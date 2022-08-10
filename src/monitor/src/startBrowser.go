package main

import (
	"monitor/browser"
	"monitor/config"
	"strconv"
)

// startBrowser launches the browser
func startBrowser(conf config.MonitorConfig) {

	url := func() string {
		u := conf.GetProxyURL() + "/"
		if u == "/" {
			u = "http://localhost:" + strconv.Itoa(conf.HTTPPort) + "/"
		}
		if conf.Token != "" {
			return u + "#/token/" + conf.Token
		}
		return u
	}

	success := browser.Open(url())

	if !success || conf.Token != "" {
		println("Please open a browser and point it to the following URL:")
		println(url())
		println("")
	}
}
