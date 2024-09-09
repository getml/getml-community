// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"net"
	"strconv"
	"time"
)

func portIsOccupied(port int) bool {
	timeout, _ := time.ParseDuration("500ms")

	conn, err := net.DialTimeout("tcp", net.JoinHostPort("localhost", strconv.Itoa(port)), timeout)

	if conn == nil && err != nil {
		return false
	}

	if conn != nil {
		conn.Close()
	}

	return true
}
