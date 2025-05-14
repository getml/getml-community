// Copyright 2025 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import "encoding/json"

type cmd struct {
	Body json.RawMessage `json:"body_"`

	Project string `json:"project_"`

	Type string `json:"type_"`
}
