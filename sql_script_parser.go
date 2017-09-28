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
	var pending = ""
	for scanner.Scan() {
		line := strings.Trim(scanner.Text(), " \t")
		if len(line) == 0 || strings.HasPrefix(line, "--") || (strings.HasPrefix(line, "/*") && strings.HasSuffix(line, "*/")) {
			pending += line
			continue
		}
		if pending != "" {
			result = append(result, pending+"\n")
		}

		var inInSingleQuote, isInDoubleQuote bool = false, false
		positionOfDelimiter := strings.Index(strings.ToLower(line), delimiterKeyword)
		if positionOfDelimiter != -1 {
			delimiter = strings.Trim(line[positionOfDelimiter+len(delimiterKeyword):], " \t")
			continue
		}
		for i := 0; i < len(line); i++ {
			aChar := line[i : i+1]

			if aChar == "'" && i > 0 && line[i-1:i] != "\\" {
				inInSingleQuote = !inInSingleQuote
			}

			if aChar == "\"" {
				isInDoubleQuote = !isInDoubleQuote
			}

			hasDelimiter, indexIncrease := hasDelimiter(line, delimiter, i)

			if hasDelimiter && !inInSingleQuote && !isInDoubleQuote {

				i = i + indexIncrease
				command = strings.Trim(command, " \t\"")

				commans := normalizeCommand(command)

				result = append(result, commans...)
				command = ""
			} else {
				command = command + aChar
			}
		}
		command = command + "\n"
	}
	return result
}

func normalizeCommand(command string) []string {
	lowerCommand := strings.ToLower(command)
	if !strings.Contains(lowerCommand, "begin") {
		return []string{command}
	}
	var result = make([]string, 0)
	positionOfEnd := strings.LastIndex(lowerCommand, "end")
	if positionOfEnd != -1 {
		endPosition := positionOfEnd + 3
		block := string(command[:endPosition])
		result = append(result, block)
		if endPosition+1 < len(command) {
			result = append(result, command[endPosition+1:])
		}
	}
	return result
}
