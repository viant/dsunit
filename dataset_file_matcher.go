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
		matchableMethod = matchableMethod[4:]
	}

	matchableMethod = convertToLowerUnderscore(matchableMethod)
	candidates, err := getFiles(getFileCandidates(baseDirectory, matchableMethod, fragment, postfixes))

	if err != nil {
		return nil, dsUnitError{fmt.Sprintf("Unable to locate files in the %v%v_%v+[%v].[json|csv|tsv]", baseDirectory, matchableMethod, fragment, postfixes) + " \n\t" + err.Error()}
	}
	return candidates, nil
}
