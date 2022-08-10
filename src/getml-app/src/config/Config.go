// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strconv"
	"strings"
)

// --------------------------------------------------------------------

// Config contains all variables that CAN be altered by the user.
type Config struct {
	Fname string `json:"-"`

	InMemory bool `json:"inMemory"`

	Monitor MonitorConfig `json:"monitor"`

	ProjectDirectory string `json:"projectDirectory"`
}

// --------------------------------------------------------------------

// GetFromFile reads the Config struct from a file.
func (c *Config) GetFromFile(fname string) {

	configStr, err := ioutil.ReadFile(fname)

	c.Fname = fname

	if err != nil {
		configStr, err = ioutil.ReadFile("../config.json")
		c.Fname = "../config.json"
	}

	if err != nil {
		panic(errors.New("config.json not found"))
	}

	decoder := json.NewDecoder(strings.NewReader(string(configStr)))

	err = decoder.Decode(c)

	if err != nil {
		panic(err)
	}

}

// --------------------------------------------------------------------

// ToFlags expresses the configurations as command line flags
// that can be passed to subprocesses.
func (c *Config) ToFlags() []string {

	flags := []string{}

	flags = append(flags, "-in-memory="+strconv.FormatBool(c.InMemory))

	flags = append(flags, "-project-directory="+c.ProjectDirectory)

	flags = append(flags, "-allow-remote-ips="+strconv.FormatBool(c.Monitor.AllowRemoteIPs))

	flags = append(flags, "-http-port="+strconv.Itoa(c.Monitor.HTTPPort))

	flags = append(flags, "-log="+strconv.FormatBool(c.Monitor.Log))

	flags = append(flags, "-tcp-port="+strconv.Itoa(c.Monitor.TCPPort))

	flags = append(flags, "-proxy-url="+c.Monitor.ProxyURL)

	flags = append(flags, "-allow-push-notifications="+strconv.FormatBool(c.Monitor.AllowPushNotifications))

	flags = append(flags, "-launch-browser="+strconv.FormatBool(c.Monitor.LaunchBrowser))

	if c.Monitor.Token != "" {
		flags = append(flags, "-token="+c.Monitor.Token)
	}

	return flags
}

// --------------------------------------------------------------------

// WriteToFile writes the Config struct to config.json
func (c *Config) WriteToFile() error {

	data, err := json.MarshalIndent(*c, "", "    ")

	if err != nil {
		return err
	}

	f, err := os.OpenFile(c.Fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	// Opening the function did work. Therefore we have to close it
	// again.
	defer f.Close()

	_, err = f.WriteString(string(data))

	return err

}

// --------------------------------------------------------------------
