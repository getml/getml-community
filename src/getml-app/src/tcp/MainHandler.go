// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"getML/data"
	"io"
	"net"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// -------------------------------------------------------------------------------------------

func handleName(nameByte []byte) (string, error) {
	if len(nameByte) == 0 {
		return "", errors.New("Project name cannot be empty!")
	}

	var name string

	err := json.Unmarshal(nameByte, &name)

	if err != nil {
		return "", err
	}

	return name, nil

}

// -------------------------------------------------------------------------------------------

// MainHandler manages the Engine processes.
type MainHandler struct {
	args []string

	projects data.Projects

	version string
}

// -------------------------------------------------------------------------------------------

// NewMainHandler is a constructor.
func NewMainHandler(version string,
	args []string,
	projects data.Projects) *MainHandler {

	return &MainHandler{
		args:     args,
		projects: projects,
		version:  version,
	}

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) bundleProject(name string, writer *zip.Writer) error {
	m.projects.Lock.RLock()

	defer m.projects.Lock.RUnlock()

	projectDirectory := filepath.FromSlash(m.projects.Config.ProjectDirectory + "/")

	path := filepath.Join(projectDirectory, name)

	zipper := m.makeZipper(name, projectDirectory, writer)

	err := filepath.Walk(path, zipper)

	if err != nil {
		return err
	}

	return nil

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) extractBundle(name string, r *zip.Reader, logger logger) (string, error) {

	var err error

	m.projects.Lock.RLock()

	defer m.projects.Lock.RUnlock()

	projectDirectory := m.projects.Config.ProjectDirectory
	firstFileName := r.File[0].Name
	bundleName := strings.Split(filepath.ToSlash(firstFileName), "/")[0]

	if name == "" {

		name = bundleName
		name, err = makeUniqueProjectName(projectDirectory, name)

		if err != nil {
			return "", err
		}

	} else if m.projects.IsRunning(name) {

		return "", errors.New("Project '" + name + "' currently loaded. To replace it, call `suspend` first.")

	}

	progressLogger, err := newProgressLogger(logger, len(r.File))

	if err != nil {
		return "", err
	}

	logger.log("Loading project...")

	for _, file := range r.File {

		fileName := file.Name

		if name != bundleName {
			fileName = strings.Replace(fileName, bundleName, name, 1)
		}

		path := filepath.Join(projectDirectory, fileName)

		err := os.MkdirAll(filepath.Dir(path), 0755)

		if err != nil {
			return "", err
		}

		fileReader, err := file.Open()

		if err != nil {
			return "", err
		}

		defer fileReader.Close()

		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())

		if err != nil {
			return "", err
		}

		defer targetFile.Close()

		_, err = io.Copy(targetFile, fileReader)

		if err != nil {
			return "", err
		}

		progressLogger.increment()

	}

	return name, nil

}

// -------------------------------------------------------------------------------------------

// Returns a function that sufficies the filepath.WalkFunc protocol. The
// function captures the name, project dir and zip writer; walks relative
// path and copies all contents in path's tree to the archive.
func (m *MainHandler) makeZipper(name string, projectDirectory string, writer *zip.Writer) filepath.WalkFunc {

	return func(path string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)

		if err != nil {
			return err
		}

		defer file.Close()

		relPath := filepath.FromSlash(strings.TrimPrefix(path, projectDirectory))

		archiveFile, err := writer.Create(relPath)

		if err != nil {
			return err
		}

		_, err = io.Copy(archiveFile, file)

		if err != nil {
			return err
		}

		return nil
	}

}

// -------------------------------------------------------------------------------------------

// DeleteProject deletes an existing project.
func (m *MainHandler) DeleteProject(nameByte []byte, c net.Conn) {

	name, err := handleName(nameByte)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	m.projects.Lock.Lock()

	defer m.projects.Lock.Unlock()

	projectDirectory := m.projects.Config.ProjectDirectory

	err = os.RemoveAll(path.Join(projectDirectory, name))

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

}

// -------------------------------------------------------------------------------------------

// Exit shuts down the getML Monitor.
func (m *MainHandler) Exit() {

	os.Exit(-1)

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) findFreePort() (int, error) {

	port := 1708

	for port < 10000 {

		if !m.projects.PortTaken(port) && !portIsOccupied(port) {
			return port, nil
		}

		port += 1
	}

	return 0, errors.New("Could not find a free port")

}

// -------------------------------------------------------------------------------------------

// IsAlive confirms that the Monitor is still alive.
func (m *MainHandler) IsAlive(c net.Conn) {
	sendString(c, "yes")
}

// -------------------------------------------------------------------------------------------

// GetStartMessage returns the start message to the Monitor.
func (m *MainHandler) GetStartMessage(c net.Conn) {

	m.projects.Lock.RLock()

	defer m.projects.Lock.RUnlock()

	msg := "The getML Monitor started successfully. " +
		"If getML runs on your local computer, open your favorite browser and go to " +
		"http://localhost:" + strconv.Itoa(m.projects.Config.Monitor.HTTPPort) + "/."

	sendString(c, msg)

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) listDirectories(directory string) []string {

	fnames := []string{}

	files, err := os.ReadDir(directory)

	if err != nil {
		return fnames
	}

	for _, f := range files {
		if f.IsDir() {
			fnames = append(fnames, f.Name())
		}

	}

	return fnames

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) loadAllPipelines(
	project string,
	enginePort int,
	c net.Conn,
	logger logger) error {

	directory := path.Join(m.projects.Config.ProjectDirectory, project, "pipelines")

	allPipelines := m.listDirectories(directory)

	if len(allPipelines) == 0 {
		return nil
	}

	progressLogger, err := newProgressLogger(logger, len(allPipelines))

	if err != nil {
		return err
	}

	logger.log("Loading pipelines...")

	for _, pipe := range allPipelines {
		loadPipelineFromDisc(pipe, enginePort)
		progressLogger.increment()
	}

	return nil

}

// -------------------------------------------------------------------------------------------

// GetVersion returns the current version.
func (m *MainHandler) GetVersion(c net.Conn) {
	sendString(c, m.version)
}

// -------------------------------------------------------------------------------------------

// loadProjectBundle loads a project bundle from memory
func (m *MainHandler) loadProjectBundle(c net.Conn, name string, data []byte, logger logger) (string, error) {

	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))

	if err != nil {
		return "", err
	}

	projectName, err := m.extractBundle(name, r, logger)

	if err != nil {
		return "", err
	}

	enginePortStr, err := m.setProject([]byte("\""+projectName+"\""), c, logger)

	if err != nil {
		return "", err
	}

	return enginePortStr, nil

}

// LoadProject loads a project bundle from disk
func (m *MainHandler) LoadProject(
	body []byte,
	c net.Conn,
	logger logger) {

	type CmdBody struct {
		Bundle string `json:"bundle_"`
		Name   string `json:"name_"`
	}

	cmdBody := CmdBody{}

	err := json.Unmarshal(body, &cmdBody)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	bundle := cmdBody.Bundle
	name := cmdBody.Name

	if ext := filepath.Ext(bundle); ext != ".getml" {
		sendString(c, "Wrong file extesion: '"+ext+"'. Can only load propper '.getml' bundles.")
		return
	}

	data, err := os.ReadFile(bundle)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	enginePortStr, err := m.loadProjectBundle(c, name, data, logger)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

	sendString(c, enginePortStr)
}

// LoadProjectBundle directly exposes loadProjectBundle and receives the data from the client
func (m *MainHandler) LoadProjectBundle(
	body []byte,
	c net.Conn,
	logger logger) {

	type CmdBody struct {
		Name string `json:"name_"`
	}

	cmdBody := CmdBody{}

	err := json.Unmarshal(body, &cmdBody)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	name := cmdBody.Name

	data, err := recvBytes(c)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	enginePortStr, err := m.loadProjectBundle(c, name, data, logger)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

	sendString(c, enginePortStr)
}

// -------------------------------------------------------------------------------------------

// ListAllProjects returns a list of all projects.
func (m *MainHandler) ListAllProjects(c net.Conn) {

	m.projects.Lock.RLock()

	defer m.projects.Lock.RUnlock()

	projectDirectory := m.projects.Config.ProjectDirectory

	projects := []string{}

	files, err := os.ReadDir(projectDirectory)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	for _, f := range files {
		if f.IsDir() {
			projects = append(projects, f.Name())
		}
	}

	response := make(map[string]([]string))

	response["projects"] = projects

	responseStr, err := json.Marshal(response)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

	sendString(c, string(responseStr))

}

// -------------------------------------------------------------------------------------------

// ListRunningProjects returns a list of all projects.
func (m *MainHandler) ListRunningProjects(c net.Conn) {

	m.projects.Lock.RLock()

	defer m.projects.Lock.RUnlock()

	response := make(map[string]([]string))

	response["projects"] = m.projects.List()

	responseStr, err := json.Marshal(response)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

	sendString(c, string(responseStr))

}

// -------------------------------------------------------------------------------------------

// RestartProject stops a project and then launches it again
func (m *MainHandler) RestartProject(
	nameByte []byte,
	c net.Conn,
	logger logger) {

	name, err := handleName(nameByte)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	err = m.projects.Stop(name)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	m.SetProject(nameByte, c, logger)

}

// -------------------------------------------------------------------------------------------

// BundleProject bundles a project and sends the bundle to the client.
func (m *MainHandler) BundleProject(
	body []byte,
	c net.Conn,
	logger logger) {

	type CmdBody struct {
		Name string `json:"name_"`
	}

	cmdBody := CmdBody{}

	err := json.Unmarshal(body, &cmdBody)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	if len(cmdBody.Name) == 0 {
		sendString(c, "Project name cannot be empty!")
		return
	}

	name := cmdBody.Name

	sink := new(bytes.Buffer)

	w := zip.NewWriter(sink)

	defer func() {
		if err := w.Close(); err != nil {
			sendString(c, err.Error())
		}

		sendString(c, "Success!")

		sendBytes(c, sink.Bytes())

	}()

	err = m.bundleProject(name, w)

	if err != nil {
		sendString(c, err.Error())
		return
	}

}

// -------------------------------------------------------------------------------------------

func (m *MainHandler) setProject(
	nameByte []byte,
	c net.Conn,
	logger logger) (string, error) {

	name, err := handleName(nameByte)

	if err != nil {
		return "", err
	}

	enginePort, err := m.findFreePort()

	if err != nil {
		return "", err
	}

	enginePortStr := strconv.Itoa(enginePort)

	args := append(m.args, "-project="+name, "-engine-port="+enginePortStr)

	cmd, err := startProcess("./engine", "./bin", m.projects.Config.Monitor.Log, args)

	if err != nil {
		return "", err
	}

	m.projects.Add(name, cmd, enginePort)

	for isRunning, _ := isAlive(enginePort); !isRunning; {
		time.Sleep(time.Millisecond * 100)
		isRunning, _ = isAlive(enginePort)
	}

	err = m.loadAllPipelines(name, enginePort, c, logger)

	if err != nil {
		return "", err
	}

	return enginePortStr, nil
}

// -------------------------------------------------------------------------------------------

// SetProject launches a new project
func (m *MainHandler) SetProject(
	nameByte []byte,
	c net.Conn,
	logger logger) {

	enginePortStr, err := m.setProject(nameByte, c, logger)

	if err != nil {
		sendString(c, err.Error())
	}

	sendString(c, "Success!")

	sendString(c, enginePortStr)
}

// -------------------------------------------------------------------------------------------

// SuspendProject shuts down a running project.
func (m *MainHandler) SuspendProject(nameByte []byte, c net.Conn) {

	name, err := handleName(nameByte)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	err = m.projects.Stop(name)

	if err != nil {
		sendString(c, err.Error())
		return
	}

	sendString(c, "Success!")

}

// -------------------------------------------------------------------------------------------

// ShutdownLocal handles a request by the Engine to shut down.
func (m *MainHandler) ShutdownLocal() {
	m.projects.KillAll()
	os.Exit(0)
}

// -------------------------------------------------------------------------------------------
