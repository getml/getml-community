// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

package tcp

import (
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"
)

func makeUniqueProjectName(projectDirectory string, projectName string) (string, error) {

	files, err := ioutil.ReadDir(projectDirectory)

	if err != nil {
		return "", err
	}

	projects := []string{}

	for _, f := range files {
		if f.Mode().IsDir() {
			projects = append(projects, f.Name())
		}
	}

	inProjects := func(name string) bool {
		for _, proj := range projects {
			if name == proj {
				return true
			}
		}

		return false
	}

	sep := "____"

	for i := 1; inProjects(projectName); i++ {
		re := regexp.MustCompile(sep + `\d+$`)
		if re.MatchString(projectName) {
			projectName = strings.SplitAfter(projectName, sep)[0] + strconv.Itoa(i)
		} else {
			projectName = projectName + sep + strconv.Itoa(i)
		}
	}

	return projectName, err

}
