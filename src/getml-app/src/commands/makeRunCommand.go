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

func makeRunCommand(
	version string,
	conf *config.Config,
	commandLine *config.CommandLine) *flag.FlagSet {

	// ------------------------------------------------------------------------

	cmd := flag.NewFlagSet("run", flag.ExitOnError)

	// ------------------------------------------------------------------------

	addHomeDirFlag(cmd, commandLine)

	cmd.StringVar(
		&conf.ProjectDirectory,
		"project-directory",
		conf.ProjectDirectory,
		"The directory in which to store all of your projects.")

	cmd.BoolVar(
		&conf.InMemory,
		"in-memory",
		conf.InMemory,
		"Whether you want the Engine to process everything in memory.")

	cmd.BoolVar(
		&conf.Monitor.AllowRemoteIPs,
		"allow-remote-ips",
		conf.Monitor.AllowRemoteIPs,
		"Whether you want to allow remote IPs to access the"+
			" http-port.")

	cmd.IntVar(
		&conf.Monitor.HTTPPort,
		"http-port",
		conf.Monitor.HTTPPort,
		"The local port of the getML Monitor."+
			" This port can only be accessed from your local computer,"+
			" unless you set allow-remote-ips=True.")

	cmd.BoolVar(
		&conf.Monitor.Log,
		"log",
		conf.Monitor.Log,
		"Whether you want the Engine log to appear in the command line."+
			" The Engine log also appears in the 'Log' page of the Monitor.")

	cmd.IntVar(
		&conf.Monitor.TCPPort,
		"tcp-port",
		conf.Monitor.TCPPort,
		"Local TCP port which serves as the communication point for the engine."+
			" This port can only be accessed from your local computer.")

	cmd.StringVar(
		&conf.Monitor.ProxyURL,
		"proxy-url",
		conf.Monitor.ProxyURL,
		"The URL of any proxy server that that redirects to the getML Monitor.")

	cmd.StringVar(
		&conf.Monitor.Token,
		"token",
		"",
		"The token used for authentication. Authentication is required when remote IPs are "+
			"allowed to access the Monitor. If authentication is required and no token is passed, "+
			"a random hexcode will be generated as the token.")

	cmd.BoolVar(
		&conf.Monitor.AllowPushNotifications,
		"allow-push-notifications",
		conf.Monitor.AllowPushNotifications,
		"Whether you want to allow the getML Monitor to send push notifications to your desktop.")

	cmd.BoolVar(
		&conf.Monitor.LaunchBrowser,
		"launch-browser",
		conf.Monitor.LaunchBrowser,
		"Whether you want to automatically launch your browser.")

	cmd.BoolVar(
		&commandLine.ForceInstall,
		"install",
		commandLine.ForceInstall,
		"Installs "+version+", even if it is already installed.")

	return cmd

	// ------------------------------------------------------------------------

}
