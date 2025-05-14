// Copyright 2025 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package main

import (
	"fmt"
	"runtime"
)

func makePackageName() string {

	packageName := fmt.Sprintf("getml-community-%s-%s-%s", version, runtime.GOARCH, runtime.GOOS)

	return packageName
}
