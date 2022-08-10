// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

func isAlive(port int) (bool, error) {

	conn, err := createConnectionToEngine(port)

	if err != nil {
		return false, err
	}

	defer conn.Close()

	err = basicCommand{Name: "", Type: "is_alive"}.send(conn)

	if err != nil {
		return false, nil
	}

	return true, nil

}
