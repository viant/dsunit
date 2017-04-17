package dsunit

import (
	"bufio"
	"io"
	"strings"
)

//parseSQLScript parses sql script and breaks it down to submittable sql statements
func ParseSQLScript(reader io.Reader) []string {
	var result = make([]string, 0)
	scanner := bufio.NewScanner(reader)
	var command, delimiter = "", ";"
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), " \t")
		if len(line) == 0 || strings.HasPrefix(line, "--") || (strings.HasPrefix(line, "/*") && strings.HasSuffix(line, "*/")) {
			continue
		}
		var inInSingleQuote, isInDoubleQuote bool = false, false
		positionOfDelimiter := strings.Index(strings.ToLower(line), delimiterKeyword)
		if positionOfDelimiter != -1 {
			delimiter = strings.Trim(line[positionOfDelimiter+len(delimiterKeyword):], " \t")
			continue
		}
		for i := 0; i < len(line); i++ {
			aChar := line[i : i+1]
			if aChar == "'" {
				inInSingleQuote = !inInSingleQuote
			}
			if aChar == "\"" {
				isInDoubleQuote = !isInDoubleQuote
			}

			hasDelimiter, indexIncrease := hasDelimiter(line, delimiter, i)
			if hasDelimiter && !inInSingleQuote && !isInDoubleQuote {

				i = i + indexIncrease
				command = strings.Trim(command, " \t\"")
				result = append(result, command)
				command = ""
			} else {
				command = command + aChar
			}
		}
		command = command + "\n"
	}
	return result
}
