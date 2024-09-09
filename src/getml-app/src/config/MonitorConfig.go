// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package config

// MonitorConfig contains all variables related to the Monitor
// that CAN be changed by the user.
type MonitorConfig struct {
	AllowPushNotifications bool `json:"allowPushNotifications"`

	AllowRemoteIPs bool `json:"allowRemoteIPs"`

	LaunchBrowser bool `json:"launchBrowser"`

	HTTPPort int `json:"httpPort"`

	// TODO: Uncomment once we have a proper login.
	// HTTPSPort int `json:"httpsPort"`

	Log bool `json:"log"`

	ProxyURL string `json:"proxyUrl"`

	TCPPort int `json:"tcpPort"`

	Token string `json:"-"`
}
