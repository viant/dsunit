package dsunit

import (
	"bufio"
	"github.com/viant/toolbox"
	"io"
	"path"
	"reflect"
	"strings"
)

var delimiterKeyword = "delimiter"

func convertValueIfNeeded(headers []string, headerTypes map[string]*reflect.Kind, rows *[][]interface{}) {
	for i, row := range *rows {
		for j, column := range headers {
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

		for i := 0; i < len(line) && index < len(row); i++ {
			aChar := line[i : i+1]

			//escape " only if value is already inside "s
			if isInDoubleQuote && ((aChar == "\\" || aChar == "\"") && i+2 < len(line)) {
				nextChar := line[i+1 : i+2]
				if nextChar == "\"" {
					i++
					fragment = fragment + nextChar
					continue
				}
			}
			//allow unescaped " be inside text if the whole text is not enclosed in "s
			if aChar == "\"" && (len(fragment) == 0 || isInDoubleQuote) {
				isInDoubleQuote = !isInDoubleQuote
				continue
			}
			if line[i:i+1] == separator && !isInDoubleQuote {
				value, valueKind := toolbox.DiscoverValueAndKind(fragment)
				row[index] = value
				if *headerTypes[headers[index]] == reflect.Invalid && *headerTypes[headers[index]] != reflect.String {
					headerTypes[headers[index]] = &valueKind
				}
				fragment = ""
				index++
				continue
			}
			fragment = fragment + aChar
		}
		if len(fragment) > 0 {
			value, valueKind := toolbox.DiscoverValueAndKind(fragment)
			row[index] = value
			if *headerTypes[headers[index]] == reflect.Invalid && *headerTypes[headers[index]] != reflect.String {
				headerTypes[headers[index]] = &valueKind
			}
		}
		rows = append(rows, row)
	}
	convertValueIfNeeded(headers, headerTypes, &rows)
	return headers, rows
}

func hasDelimiter(line, delimiter string, index int) (contains bool, indexIncrease int) {
	if !(index+len(delimiter) <= len(line)) {
		return false, 0
	}

	if line[index:index+len(delimiter)] == delimiter {
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
		aChar := upperCamelCase[i : i+1]
		if strings.ToUpper(aChar) == aChar && !(aChar >= "0" && aChar <= "9") {
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
		return file[0 : len(file)-extensionLength]
	}
	return file
}
