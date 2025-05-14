// Copyright 2025 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"bytes"
	"encoding/binary"
	"io"
)

func sendBytes(conn io.Writer, data []byte) error {
	length := uint64(len(data))

	if err := binary.Write(conn, binary.BigEndian, length); err != nil {
		return err
	}

	_, err := io.Copy(conn, bytes.NewReader(data))
	return err
}
