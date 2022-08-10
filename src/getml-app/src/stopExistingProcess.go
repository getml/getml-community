// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package main

func stopExistingProcess(tcpPort int) error {

	cmd := `{"type_":"shutdownlocal"}`

	conn, err := createConnectionToEngine(tcpPort)

	if err != nil {
		return err
	}

	err = sendString(conn, cmd)

	if err != nil {
		return err
	}

	return nil
}
