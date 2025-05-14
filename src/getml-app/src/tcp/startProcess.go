// Copyright 2025 Code17 GmbH
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

	// unsetting LD_LIBRARY_PATH for engine subprocesses because we ship our own
	// libstdc++ et al. with the engine, setting LD_LIBRARY_PATH like e.g. in
	// VertexAI breaks dynamic linking of our libraries
	cmd.Env = append(cmd.Environ(), "LD_LIBRARY_PATH=")

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
