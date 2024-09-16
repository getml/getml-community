// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"os"
	"os/exec"
)

func startProcess(fname string, workDir string, log bool, args []string) (*exec.Cmd, error) {

	cmd := exec.Command(fname, args...)
	cmd.Dir = workDir

	if log {
		cmd.Stdout = os.Stdout
	}

	cmd.Stderr = os.Stderr

	err := cmd.Start()

	if err != nil {
		return nil, err
	}

	return cmd, nil
}
