// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package data

import (
	"getML/config"
	"os/exec"
	"sync"
)

// ----------------------------------------------------

// StandardVariables contains common variables that are included
// in all templates.
type StandardVariables struct {
	Cmd *exec.Cmd

	Config *config.Config

	CurrentProjectName *string

	EnginePort *int

	LastPopulationValidation *string

	// Random number drawn at login and used to identify the usage of
	// the software by the user.
	LicenseSeedDynamic string

	Lock *sync.RWMutex
}

// ----------------------------------------------------

// NewStandardVariables is a constructor.
func NewStandardVariables(
	cmd *exec.Cmd,
	currentProjectName string,
	config *config.Config,
	licenseSeedDynamic string,
	enginePort int) StandardVariables {

	lastPopulationValidation := ""

	return StandardVariables{
		Cmd:                      cmd,
		Config:                   config,
		CurrentProjectName:       &currentProjectName,
		EnginePort:               &enginePort,
		LastPopulationValidation: &lastPopulationValidation,
		LicenseSeedDynamic:       licenseSeedDynamic,
		Lock:                     &sync.RWMutex{},
	}
}

// ----------------------------------------------------
