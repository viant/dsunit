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
	"bufio"
	"io"
	"strings"
	"reflect"
	"github.com/viant/toolbox"
	"path"
)

var delimiterKeyword = "delimiter"




//parseSQLScript parses sql script and breaks it down to submittable sql statements
func parseSQLScript(reader io.Reader) []string {
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
			delimiter = strings.Trim(line[positionOfDelimiter + len(delimiterKeyword): len(line)], " \t")
			continue
		}
		for i := 0; i < len(line); i++ {
			aChar := line[i:i + 1]
			if aChar == "'" {
				inInSingleQuote = ! inInSingleQuote
			}
			if aChar == "\"" {
				isInDoubleQuote = ! isInDoubleQuote
			}

			hasDelimiter, indexIncrease := hasDelimiter(line, delimiter, i)
			if hasDelimiter && ! inInSingleQuote && ! isInDoubleQuote {


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





func convertValueIfNeeded(headers []string, headerTypes  map[string]*reflect.Kind, rows *[][]interface{}) {
	for i, row := range  *rows {
		for j, column:= range headers {
			if (*rows)[i][j] == nil {
				continue
			}
			kindType := *headerTypes[column]
			value := row[j]
			if reflect.ValueOf(value).Kind() == kindType {
				continue
			}
			(*rows)[i][j] = toolbox.AsString(value)
		}
	}
}



func parseColumnarData(reader io.Reader, separator string) ([]string, [][]interface{}) {
	var rows = make([][]interface{}, 0)
	var headerTypes = make(map[string]*reflect.Kind)
	scanner := bufio.NewScanner(reader)
	var headers = make([]string, 0)

	if scanner.Scan() {
		for _, header := range strings.Split(scanner.Text(), separator) {
			invalid := reflect.Invalid
			headerTypes[header] = &invalid
			headers = append(headers, header)
		}
	}
	for scanner.Scan() {
		var isInDoubleQuote = false
		var index = 0
		line := scanner.Text()
		var fragment = ""
		var row = make([]interface{}, len(headerTypes))

		for i := 0; i < len(line) && index < len(row) ; i++ {
			aChar := line[i:i + 1]

			//escape " only if value is already inside "s
			if isInDoubleQuote && aChar== "\\" && i + 1 <	 len(line) {
				nextChar := line[i+1:i+2]
				if nextChar == "\"" {
					i++
					fragment = fragment + nextChar
					continue
				}
			}
			//allow unescaped " be inside text if the whole text is not enclosed in "s
			if aChar == "\"" && (len(fragment) == 0 || isInDoubleQuote)   {
				isInDoubleQuote = !isInDoubleQuote
				continue
			}
			if line[i:i + 1] == separator && ! isInDoubleQuote {
				value, valueKind:= toolbox.DiscoverValueAndKind(fragment)
				row[index] = value
				if *headerTypes[headers[index]]== reflect.Invalid && *headerTypes[headers[index]] != reflect.String {
					headerTypes[headers[index]]=&valueKind
				}
				fragment = ""
				index++
				continue
			}
			fragment = fragment + aChar
		}
		if len(fragment) > 0 {
			value, valueKind:= toolbox.DiscoverValueAndKind(fragment)
			row[index] = value
			if *headerTypes[headers[index]]== reflect.Invalid && *headerTypes[headers[index]] != reflect.String {
				headerTypes[headers[index]]=&valueKind
			}
		}
		rows = append(rows, row)
	}
	convertValueIfNeeded(headers, headerTypes, &rows)
	return headers, rows
}



func hasDelimiter(line, delimiter string, index int) (contains bool, indexIncrease int) {
	if !(index + len(delimiter) <= len(line)) {
		return false, 0
	}

	if line[index:index + len(delimiter)] == delimiter {
		return true, len(delimiter) - 1
	}
	return false, 0
}


func convertToLowerUnderscore(upperCamelCase string) string {
	if len(upperCamelCase) == 0 {
		return ""
	}
	result := strings.ToLower(upperCamelCase[0:1])
	for i := 1; i < len(upperCamelCase); i++ {
		aChar := upperCamelCase[i:i + 1]
		if strings.ToUpper(aChar) == aChar && ! (aChar >= "0" && aChar <= "9")  {
			result = result + "_" + strings.ToLower(aChar)
		} else {
			result = result + aChar
		}
	}
	return result
}

func removeFileExtension(file string) string {
	extensionLength := len(path.Ext(file))
	if extensionLength > 0 {
		return file[0:len(file) - extensionLength]
	}
	return file
}
