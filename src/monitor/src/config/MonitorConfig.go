package config

// MonitorConfig contains all variables related to the monitor
// that CAN be changed by the user.
type MonitorConfig struct {
	AllowPushNotifications bool `json:"allowPushNotifications"`

	AllowRemoteIPs bool `json:"allowRemoteIPs"`

	LaunchBrowser bool `json:"launchBrowser"`

	Log bool `json:"log"`

	HTTPPort int `json:"httpPort"`

	HTTPSPort int `json:"httpsPort"`

	ProxyURL string `json:"proxyUrl"`

	NoLogin bool `json:"noLogin"`

	TCPPort int `json:"tcpPort"`

	Token string `json:"-"`
}

// GetProxyURL is a getter that gets rid of
// trailing slashes.
func (m MonitorConfig) GetProxyURL() string {

	p := m.ProxyURL

	if len(p) > 0 && p[len(p)-1:] == "/" {
		p = p[:len(p)-1]
	}

	return p
}
