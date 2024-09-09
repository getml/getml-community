// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"encoding/binary"
	"io"
)

// sendString sends a string to the given connection
func sendString(conn io.Writer, str string) error {

	length := int32(len(str))

	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return err
	}

	_, err := io.WriteString(conn, str)

	return err

}

func SendString(conn io.Writer, str string) error {
	err := sendString(conn, str)
	if err != nil {
		return err
	}
	return nil
}
