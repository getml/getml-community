// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"net"
)

func sendCommand(command command, conn *net.TCPConn) error {

	cmd, err := marshalCommand(&command)

	if err != nil {
		return err
	}

	err = sendString(conn, cmd)

	if err != nil {
		return err
	}

	return nil

}
