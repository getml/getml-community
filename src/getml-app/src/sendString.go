// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package main

import (
	"encoding/binary"
	"io"
)

// sendString sends a string to the getML engine.
func sendString(conn io.Writer, str string) error {

	byteString := []byte(str)

	length := len(byteString)

	err := binary.Write(conn, binary.BigEndian, int32(length))

	if err != nil {
		return err
	}

	nBytesSent := 0

	for nBytesSent < length {
		n, err := conn.Write(byteString[nBytesSent:])

		nBytesSent += n

		if err != nil {
			return err
		}
	}

	return nil

}
