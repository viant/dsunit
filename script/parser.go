package script

import (
	"github.com/viant/toolbox"
	"io"
	"io/ioutil"
	"strings"
)

const (
	eofToken     = -1
	invalidToken = iota
	whitespaces
	lineBreak
	commandTerminator
	delimiterKeyword
	createKeyword
	beginKeyword
	functionKeyword
	orKeyword
	replaceKeyword
	commandEnd
	pgDelimiter
	plSQLBlock
)

var matchers = map[int]toolbox.Matcher{
	commandTerminator: toolbox.NewTerminatorMatcher(";"),
	commandEnd:        toolbox.NewKeywordsMatcher(false, ";"),
	delimiterKeyword:  toolbox.NewKeywordsMatcher(false, "delimiter"),
	pgDelimiter:       toolbox.NewTerminatorMatcher("$$"),
	plSQLBlock:        toolbox.NewBodyMatcher("BEGIN", "END;"),
	beginKeyword:      toolbox.NewTerminatorMatcher("BEGIN"),
	createKeyword:     toolbox.NewKeywordsMatcher(false, "create"),
	orKeyword:         toolbox.NewKeywordsMatcher(false, "or"),
	replaceKeyword:    toolbox.NewKeywordsMatcher(false, "replace"),

	functionKeyword: toolbox.NewKeywordsMatcher(false, "function"),
	whitespaces:     toolbox.CharactersMatcher{" \n\t"},
	lineBreak:       toolbox.CharactersMatcher{"\n"},
}

//ParseWithReader splits SQL blob into separate commands
func ParseWithReader(reader io.Reader) []string {
	var result = make([]string, 0)
	data, _ := ioutil.ReadAll(reader)
	if len(data) == 0 {
		return result
	}
	return Parse(string(data))
}

//Parse splits SQL blob into separate commands
func Parse(expression string) []string {
	var result = make([]string, 0)
	tokenizer := toolbox.NewTokenizer(expression, invalidToken, eofToken, matchers)

	pending := ""
	appendMatched := func(text string) {
		SQL := strings.TrimSpace(pending + text)
		if SQL != "" {
			result = append(result, SQL)
		}
		pending = ""
	}

	done := false

outer:
	for tokenizer.Index < len(expression) && !done {

		match := tokenizer.Nexts(whitespaces, createKeyword, delimiterKeyword, plSQLBlock, commandTerminator, commandEnd, eofToken)
		switch match.Token {
		case whitespaces:
			pending += match.Matched
		case delimiterKeyword:
			if match := tokenizer.Nexts(lineBreak, eofToken); match.Token == lineBreak {
				delimiter := string(match.Matched[:len(match.Matched)-1])
				remaining := string(tokenizer.Input[tokenizer.Index:])
				if index := strings.Index(remaining, delimiter); index != -1 {
					match := remaining[0:index]
					tokenizer.Index += len(match)
					appendMatched(match)
				}
			}
		case createKeyword:
			pending += match.Matched
			if match := tokenizer.Nexts(whitespaces, eofToken); match.Token == whitespaces {

				pending += match.Matched

				candidates := []int{orKeyword, whitespaces, replaceKeyword, whitespaces, functionKeyword, beginKeyword, orKeyword}

				match := tokenizer.Nexts(candidates...)

				for ; len(candidates) > 3; {
					if match.Token != candidates[0] {
						break
					}
					pending += match.Matched
					candidates = candidates[1:]
					match = tokenizer.Nexts(candidates...)
				}

				switch match.Token {

				case functionKeyword:
					pending += match.Matched
					if match = tokenizer.Nexts(pgDelimiter, eofToken); match.Token == pgDelimiter {
						pending += match.Matched + "$$"
						tokenizer.Index += 2
						if match = tokenizer.Nexts(pgDelimiter, eofToken); match.Token == pgDelimiter {
							pending += match.Matched + "$$"
							tokenizer.Index += 2
						}
					}
				case beginKeyword:
					if strings.Contains(match.Matched, ";") || strings.Contains(match.Matched, "$$") {
						tokenizer.Index -= len(match.Matched)
						continue
					}
					pending += match.Matched
					continue

				}
			}
		case eofToken:
			pending += match.Matched
		case plSQLBlock:
			pending += string(match.Matched[:len(match.Matched)-1])
			appendMatched("")
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
