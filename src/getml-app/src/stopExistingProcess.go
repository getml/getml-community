// Copyright 2025 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"getML/tcp"
)

func stopExistingProcess(tcpPort int) error {

	cmd := `{"type_":"shutdownlocal"}`

	conn, err := createConnectionToEngine(tcpPort)

	if err != nil {
		return err
	}

	err = tcp.SendString(conn, cmd)

	if err != nil {
		return err
	}

	return nil
}
