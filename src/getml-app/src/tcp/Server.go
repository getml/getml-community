// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

package tcp

import (
	"getML/data"
	"log"
	"net"
	"strconv"
)

// -------------------------------------------------------------

// Server is the the TCP server that communicates
// with the Engine.
type Server struct {
	mainHandler *MainHandler

	projects data.Projects
}

// -------------------------------------------------------------

// NewServer is a constructor.
func NewServer(
	mainHandler *MainHandler,
	projects data.Projects) *Server {

	return &Server{
		mainHandler: mainHandler,
		projects:    projects,
	}

}

// -------------------------------------------------------------

// Launch launches a tcp server on the TcpPort.
func (s *Server) Launch() {

	port := s.projects.Config.Monitor.TCPPort

	l, err := net.Listen("tcp", "localhost:"+strconv.Itoa(port))

	if err != nil {
		log.Println("Unable to start up TCP server at port",
			strconv.Itoa(port), "-", err.Error())
		return
	}

	defer l.Close()

	for {
		c, err := l.Accept()

		if err != nil {
			log.Println(err.Error())
			continue
		}

		go s.handleConnection(c)
	}

}

// -------------------------------------------------------------

func (s *Server) handleConnection(c net.Conn) {

	defer c.Close()

	cmd, err := recvCmd(c)

	if err != nil {
		log.Println("Error receiving command:", err.Error())
		_, err := c.Write([]byte(err.Error()))
		if err != nil {
			return
		}
		return
	}

	body := []byte(cmd.Body)

	switch cmd.Type {

	case "deleteproject":
		s.mainHandler.DeleteProject(body, c)

	case "exit":
		s.mainHandler.Exit()

	case "getversion":
		s.mainHandler.GetVersion(c)

	case "isalive":
		s.mainHandler.IsAlive(c)

	case "listallprojects":
		s.mainHandler.ListAllProjects(c)

	case "listrunningprojects":
		s.mainHandler.ListRunningProjects(c)

	case "loadproject":
		logger := newLogger(c)
		s.mainHandler.LoadProject(body, c, *logger)

	case "loadprojectbundle":
		logger := newLogger(c)
		s.mainHandler.LoadProjectBundle(body, c, *logger)

	case "restartproject":
		logger := newLogger(c)
		s.mainHandler.RestartProject(body, c, *logger)

	case "bundleproject":
		logger := newLogger(c)
		s.mainHandler.BundleProject(body, c, *logger)

	case "setproject":
		logger := newLogger(c)
		s.mainHandler.SetProject(body, c, *logger)

	case "shutdownlocal":
		s.mainHandler.ShutdownLocal()

	case "suspendproject":
		s.mainHandler.SuspendProject(body, c)

	default:
		sendString(c, "Unknown command: '"+cmd.Type+"'")
	}
}

// -------------------------------------------------------------
