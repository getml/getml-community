// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"errors"
)

func loadPipelineFromDisc(name string, port int) error {

	conn, err := createConnectionToEngine(port)

	if err != nil {
		return err
	}

	defer conn.Close()

	err = basicCommand{
		Name: name,
		Type: "Pipeline.load"}.send(conn)

	if err != nil {
		return err
	}

	msg, err := recvString(conn)

	if err != nil {
		return err
	}

	if msg != "Success!" {
		return errors.New(msg)
	}

	return nil

}
