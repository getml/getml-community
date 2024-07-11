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

func recvBytes(conn io.Reader) ([]byte, error) {

	var length int32

	err := binary.Read(conn, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, length)

	_, err = io.ReadFull(conn, bytes)
	if err != nil {
		return nil, err
	}

	return bytes, nil
}
