// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import "encoding/json"

// Parses a command
func marshalCommand(command *command) (string, error) {

	cmdStr, err := json.Marshal(command)

	if err != nil {
		return "", err
	}

	return string(cmdStr), nil

}
