// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import "net"

type basicCommand struct {
	Name string `json:"name_"`

	Type string `json:"type_"`
}

func (b basicCommand) send(conn *net.TCPConn) error {
	return sendCommand(b, conn)
}
