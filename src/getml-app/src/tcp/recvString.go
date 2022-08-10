// Copyright 2022 The SQLNet Company GmbH
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

func recvString(conn io.Reader) (string, error) {

	length := int32(0)

	err := binary.Read(conn, binary.BigEndian, &length)

	if err != nil {
		return "", err
	}

	bytes := make([]byte, length)

	for i := int32(0); i < length; i++ {
		binary.Read(conn, binary.BigEndian, &bytes[i])
	}

	if err != nil {
		return "", err
	}

	return string(bytes), nil

}
