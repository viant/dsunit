/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package dsunit

import (
	"fmt"
	"os"
	"strings"
)

var testDatasetFileFormats = []string{"json", "csv", "tsv"}

func getFileCandidates(baseDirectory string, method string, fragment string, postfixes []string) []string {
	var result = make([]string, 0)
	for _, format := range testDatasetFileFormats {
		for _, postfix := range postfixes {
			result = append(result, baseDirectory+method+"_"+fragment+"_"+postfix+"."+format)
		}
	}
	return result
}

func getFiles(candidates []string) ([]string, error) {
	var urls = make([]string, 0)
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); !os.IsNotExist(err) {
			urls = append(urls, candidate)
		}
	}
	if len(urls) == 0 {
		return nil, dsUnitError{fmt.Sprintf("Failed to locate files: candidates %v", candidates)}
	}
	return urls, nil
}

func matchFiles(baseDirectory string, method string, fragment string, postfixes []string) ([]string, error) {
	matchableMethod := method
	if strings.HasPrefix(matchableMethod, "Test") {
		matchableMethod = matchableMethod[4:len(matchableMethod)]
	}

	matchableMethod = convertToLowerUnderscore(matchableMethod)
	candidates, err := getFiles(getFileCandidates(baseDirectory, matchableMethod, fragment, postfixes))

	if err != nil {
		return nil, dsUnitError{fmt.Sprintf("Unable to locate files in the %v%v_%v+[%v].[json|csv|tsv]", baseDirectory, matchableMethod, fragment, postfixes) + " \n\t" + err.Error()}
	}
	return candidates, nil
}
