package script

import (
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"regexp"
	"strings"
)

const (
	eofToken     = -1
	invalidToken = iota
	whitespaces
	lineBreak
	lineBreakTerminator
	commandTerminator
	quoteTerminator
	delimiterKeyword
	createKeyword
	beginKeyword
	functionKeyword
	orKeyword
	replaceKeyword
	commandEnd
	pgDelimiter
	plSQLBlock
	inlineComment
)

//ParseWithReader splits SQL blob into separate commands
func ParseWithReader(reader io.Reader) []string {
	var result = make([]string, 0)
	data, _ := ioutil.ReadAll(reader)
	if len(data) == 0 {
		return result
	}
	return Parse(string(data))
}

func appendMatched(terminator, pending *string, result *[]string) func(text string) {
	return func(text string) {
		SQL := strings.TrimSpace(*pending + text)
		SQL = regexp.MustCompile(`--.*\n`).ReplaceAllString(SQL, "")
		SQL = regexp.MustCompile(`--.*\r`).ReplaceAllString(SQL, "")
		quotesCount := strings.Count(SQL, `'`) - strings.Count(SQL, `\'`)
		if quotesCount % 2 == 1 { //missing closing quote
			*pending = SQL + *terminator
			return
		}
		if SQL != "" {
			*result = append(*result, SQL)
		}
		*pending = ""

	}
}

//Parse splits SQL blob into separate commands
func Parse(expression string) []string {
	result := parse(expression, ";", false)
	return result
}

//Parse splits SQL blob into separate commands
func parse(expression string, terminator string, delimiterMode bool) []string {
	var result = make([]string, 0)

	var matchers = map[int]toolbox.Matcher{
		commandTerminator: toolbox.NewTerminatorMatcher(terminator),
		commandEnd:        toolbox.NewKeywordsMatcher(false, terminator),
		quoteTerminator:   toolbox.NewTerminatorMatcher(`'`),
		delimiterKeyword:  toolbox.NewKeywordsMatcher(false, "delimiter"),
		pgDelimiter:       toolbox.NewTerminatorMatcher("$$"),
		plSQLBlock:        toolbox.NewSQLBeginEndMatcher(),
		inlineComment:     toolbox.NewBodyMatcher("--", "\n"),
		beginKeyword:      toolbox.NewTerminatorMatcher("BEGIN"),
		createKeyword:     toolbox.NewKeywordsMatcher(false, "create"),
		orKeyword:         toolbox.NewKeywordsMatcher(false, "or"),
		replaceKeyword:    toolbox.NewKeywordsMatcher(false, "replace"),
		lineBreakTerminator: toolbox.NewTerminatorMatcher("\n"),
		functionKeyword: toolbox.NewKeywordsMatcher(false, "function"),
		whitespaces:     toolbox.CharactersMatcher{" \n\t"},
		lineBreak:       toolbox.CharactersMatcher{"\n"},
	}

	tokenizer := toolbox.NewTokenizer(expression, invalidToken, eofToken, matchers)

	pending := ""
	appendMatched := appendMatched(&terminator, &pending, &result)

outer:
	for tokenizer.Index < len(expression) {
		match := tokenizer.Nexts(whitespaces, inlineComment, createKeyword, delimiterKeyword, plSQLBlock, commandTerminator, commandEnd, eofToken)
		switch match.Token {
		case whitespaces:
			pending += match.Matched
		case inlineComment:
			appendMatched("")
		case delimiterKeyword:
			if match := tokenizer.Nexts(lineBreakTerminator, eofToken); match.Token == lineBreakTerminator {
				delimiter := strings.TrimSpace(string(match.Matched[:len(match.Matched)]))
				remaining := string(tokenizer.Input[tokenizer.Index:])

				index := strings.Index(strings.ToLower(remaining), "\ndelimiter")
				if index == -1 {
					index = strings.Index(strings.ToLower(remaining), "\rdelimiter")
				}
				if index != -1 {
					delimitedStatements := string(remaining[0:index])
					commands := parse(delimitedStatements, delimiter, true)
					for i := 0; i < len(commands); i++ {
						appendMatched(commands[i])
					}
					tokenizer.Index += len(delimitedStatements)
				}
			}

		case createKeyword:
			pending += match.Matched

			if match := tokenizer.Nexts(whitespaces, eofToken); match.Token == whitespaces {
				pending += match.Matched
				candidates := []int{orKeyword, whitespaces, inlineComment, replaceKeyword, whitespaces, functionKeyword, beginKeyword, orKeyword}
				match := tokenizer.Nexts(candidates...)

				for len(candidates) > 3 {
					if match.Token != candidates[0] {
						break
					}
					pending += match.Matched
					candidates = candidates[1:]
					match = tokenizer.Nexts(candidates...)
				}

				switch match.Token {

				case inlineComment:
					appendMatched("")
				case functionKeyword:
					pending += match.Matched
				case beginKeyword:
					if strings.Contains(match.Matched, ";") || strings.Contains(match.Matched, "$$") {
						tokenizer.Index -= len(match.Matched)
						continue
					}
					pending += match.Matched
					continue
				default:
					pending += match.Matched

				}
			}
		case pgDelimiter:
			pending += match.Matched
			appendMatched("")
			tokenizer.Index += len("$$")
		case eofToken:
			pending += match.Matched
		case plSQLBlock:
			pending += string(match.Matched[:len(match.Matched)-1])
			if delimiterMode {
				appendMatched("")
			} else {
				appendMatched(";")
			}
		case invalidToken:
			break outer
		case commandTerminator:
			pending += match.Matched

		case commandEnd:
			appendMatched("")

		}

	}
	appendMatched("")
	return result
}
