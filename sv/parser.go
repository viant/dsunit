package sv

import (
	"bufio"
	"github.com/viant/toolbox"
	"reflect"
	"strings"
)

//SeparatedValueParser represents separated value parser, it discover and convert undelying data
type SeparatedValueParser struct {
	factory   toolbox.DecoderFactory
	delimiter string
}

func (p *SeparatedValueParser) Parse(data []byte) ([]map[string]interface{}, error) {
	text := strings.Replace(string(data), "\r", "", len(data))
	scanner := bufio.NewScanner(strings.NewReader(text))
	record := &toolbox.DelimitedRecord{Delimiter: p.delimiter}
	if scanner.Scan() {
		p.factory.Create(strings.NewReader(scanner.Text())).Decode(record)
	}
	var result = make([]map[string]interface{}, 0)
	for scanner.Scan() {
		var line = scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}
		record.Record = make(map[string]interface{})
		if err := p.factory.Create(strings.NewReader(line)).Decode(record); err != nil {
			return nil, err
		}

		result = append(result, record.Record)
	}
	p.discoverDataTypes(record.Columns, result)
	return result, nil
}

func (p *SeparatedValueParser) discoverDataTypes(columns []string, records []map[string]interface{}) {
	var columnKinds = make(map[string]reflect.Kind)
	for _, column := range columns {
		columnKinds[column] = reflect.Invalid
	}
	for _, record := range records {
		for column, value := range record {
			if columnKinds[column] == reflect.String {
				continue
			}
			textValue := toolbox.AsString(value)
			if textValue == "" {
				continue
			}
			discovered, kind := toolbox.DiscoverValueAndKind(textValue)
			record[column] = discovered
			if kind == reflect.Int && columnKinds[column] == reflect.Float64 {
				continue
			}
			columnKinds[column] = kind
		}
	}
	for _, record := range records {
		for column, value := range record {
			switch columnKinds[column] {
			case reflect.Int:
				record[column] = toolbox.AsInt(value)
			case reflect.Float64:
				record[column] = toolbox.AsFloat(value)
			case reflect.Bool:
				record[column] = toolbox.AsBoolean(value)
			default:
				record[column] = toolbox.AsString(value)
			}
		}
	}
}

func NewSeparatedValueParser(delimiter string) *SeparatedValueParser {
	return &SeparatedValueParser{
		delimiter: delimiter,
		factory:   toolbox.NewDelimiterDecoderFactory(),
	}
}
