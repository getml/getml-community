// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"io"
)

func recvString(conn io.Reader) (string, error) {

	bytes, err := recvBytes(conn)

	if err != nil {
		return "", err
	}

	return string(bytes), nil

}
