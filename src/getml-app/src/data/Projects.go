// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package data

import (
	"errors"
	"fmt"
	"getML/config"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// ----------------------------------------------------

// Projects is a handler for all projects.
type Projects struct {
	Config *config.Config

	// Random number drawn at login and used to identify the usage of
	// the software by the user.
	LicenseSeedDynamic string

	Lock *sync.RWMutex

	stdVarsMap *map[string]StandardVariables

	VersionNumber string
}

// ----------------------------------------------------

// NewProjects is a constructor.
func NewProjects(
	config *config.Config,
	versionNumber string) Projects {

	rand.Seed(time.Now().Unix())

	licenseSeedDynamic := fmt.Sprintf("%.10f", rand.Float64())

	stdVarsMap := make(map[string]StandardVariables)

	return Projects{
		Config:             config,
		LicenseSeedDynamic: licenseSeedDynamic,
		Lock:               &sync.RWMutex{},
		stdVarsMap:         &stdVarsMap,
		VersionNumber:      versionNumber,
	}

}

// --------------------------------------------------------

// Add adds a new project.
func (p *Projects) Add(project string, cmd *exec.Cmd, port int) {

	p.Lock.Lock()

	defer p.Lock.Unlock()

	(*p.stdVarsMap)[project] = NewStandardVariables(
		cmd,
		project,
		p.Config,
		p.LicenseSeedDynamic,
		port)

	log.Print("Launched project '"+project+"'. Port: ", port, ", PID: ", cmd.Process.Pid, ".")

}

// --------------------------------------------------------

// GetStandardVars returns the standard variables related to the
// project.
func (p Projects) GetStandardVars(project string) (*StandardVariables, error) {
	p.Lock.RLock()

	defer p.Lock.RUnlock()

	stdVars, found := (*p.stdVarsMap)[project]

	if !found {
		return nil, errors.New("No project called '" + project + "' is currently running.")
	}

	return &stdVars, nil

}

// --------------------------------------------------------

// IsRunning signifies whether a project is currently running.
func (p Projects) IsRunning(project string) bool {
	p.Lock.RLock()

	defer p.Lock.RUnlock()

	_, found := (*p.stdVarsMap)[project]

	return found
}

// --------------------------------------------------------

// List lists all projects currently running.
func (p *Projects) List() []string {

	p.Lock.RLock()

	defer p.Lock.RUnlock()

	list := []string{}

	for key := range *p.stdVarsMap {
		list = append(list, key)
	}

	return list
}

// --------------------------------------------------------

// PortTaken checks whether a particular port is taken
// by any process known to the monitor.
func (p *Projects) PortTaken(port int) bool {

	if port == p.Config.Monitor.HTTPPort {
		return true
	}

	if port == p.Config.Monitor.TCPPort {
		return true
	}

	p.Lock.RLock()

	defer p.Lock.RUnlock()

	for _, stdVars := range *p.stdVarsMap {
		if port == *stdVars.EnginePort {
			return true
		}
	}

	return false

}

// --------------------------------------------------------

func (p *Projects) deleteTempDir(stdVars StandardVariables) error {

	path := filepath.Join(
		stdVars.Config.ProjectDirectory,
		*stdVars.CurrentProjectName,
		"tmp")

	return os.RemoveAll(path)

}

// --------------------------------------------------------

// Stop stops a running project
func (p *Projects) Stop(project string) error {

	p.Lock.Lock()

	defer p.Lock.Unlock()

	stdVars, found := (*p.stdVarsMap)[project]

	if !found {
		return errors.New("No project called '" + project + "' is currently running.")
	}

	stdVars.Cmd.Process.Signal(syscall.SIGINT)

	p.deleteTempDir(stdVars)

	delete(*p.stdVarsMap, project)

	log.Print("Suspended project '" + project + "'.")

	return nil
}

// --------------------------------------------------------

// KillAll kills all running projects without checking
// whether they were shut down.
func (p *Projects) KillAll() {

	allProjects := p.List()

	for _, project := range allProjects {
		p.Stop(project)
	}

}

// --------------------------------------------------------
