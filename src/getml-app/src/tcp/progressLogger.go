// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"errors"
	"strconv"
	"strings"
)

type progressLogger struct {
	logger *logger

	numIterations int

	totalNumIterations int
}

func newProgressLogger(
	logger logger,
	totalNumIterations int) (*progressLogger, error) {

	if totalNumIterations <= 0 {
		return nil, errors.New(
			"'totalNumIterations' needs to be greater than 0")
	}

	return &progressLogger{
		logger:             &logger,
		numIterations:      0,
		totalNumIterations: totalNumIterations,
	}, nil

}

func (l *progressLogger) log(msg string) {
	if strings.Contains(msg, "Progress:") {
		l.increment()
	}
}

func (l *progressLogger) increment() {
	l.numIterations++
	progress := (100 * l.numIterations) / l.totalNumIterations
	l.logger.log("Progress: " + strconv.Itoa(progress) + "%")
}

func (l *progressLogger) numRemaining() int {

	if l.numIterations > l.totalNumIterations {
		return 0
	}

	return l.totalNumIterations - l.numIterations

}
