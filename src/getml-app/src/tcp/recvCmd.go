// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"encoding/json"
	"net"
)

func recvCmd(c net.Conn) (cmd, error) {

	cmd := cmd{}

	cmdStr, err := recvString(c)

	if err != nil {
		return cmd, err
	}

	err = json.Unmarshal([]byte(cmdStr), &cmd)

	return cmd, err
}
