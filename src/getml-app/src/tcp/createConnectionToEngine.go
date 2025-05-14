// Copyright 2025 Code17 GmbH
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

// CreateConnectionToEngine creates a low-level socket connection to the getML Engine
func createConnectionToEngine(port int) (*net.TCPConn, error) {

	tcpAddr, err := net.ResolveTCPAddr("tcp4", "localhost:"+strconv.Itoa(port))

	if err != nil {
		return nil, err
	}

	for i := 0; i < 5; i++ {

		conn, err := net.DialTCP("tcp", nil, tcpAddr)

		if err == nil {
			return conn, nil
		} else if i == 4 {
			return nil, err
		}

		time.Sleep(time.Second)

	}

	return nil, err

}
