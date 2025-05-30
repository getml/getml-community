// Copyright 2025 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"net"
)

type logger struct {
	c net.Conn
}

func newLogger(c net.Conn) *logger {
	return &logger{c: c}
}

func (l *logger) log(msg string) {
	sendString(l.c, "log: "+msg)
}
