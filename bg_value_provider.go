package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

type bg_sequence struct {
	seq map[string]int64
}

type positionValueProvider struct{}

func (p *positionValueProvider) countInsertable(dataset *Dataset) int64 {
	var result = 0
	for _, row := range dataset.Rows {
		for _, column := range dataset.Columns {
			value := row.Value(column)
			if textValue, ok := value.(string); ok {
				if strings.Contains(textValue, ":pos ") {
					result++
				}
			}
		}
	}
	return int64(result)
}

func (p *positionValueProvider) fetchSequence(context toolbox.Context, sequenceName string) (int64, error) {
	manager := *context.GetOptional((*dsc.Manager)(nil)).(*dsc.Manager)
	dataset := context.GetOptional((*Dataset)(nil)).(*Dataset)
	bgDialectable := *context.GetOptional((*dsc.DatastoreDialect)(nil)).(*dsc.DatastoreDialect)
	seq, err := bgDialectable.GetSequence(manager, sequenceName)
	if err != nil {
		return 0, err
	}
	insertableCount := p.countInsertable(dataset)

	result := seq - insertableCount
	return result, nil
}

func (p *positionValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	sequenceName := toolbox.AsString(arguments[0])

	if !context.Contains((*bg_sequence)(nil)) {
		seq, err := p.fetchSequence(context, sequenceName)
		if err != nil {
			return nil, err
		}
		var sequenceValue = bg_sequence{seq: make(map[string]int64)}
		sequenceValue.seq[sequenceName] = seq
		context.Put((*bg_sequence)(nil), &sequenceValue)
	}
	var sequence = context.GetOptional((*bg_sequence)(nil)).(*bg_sequence)
	result := sequence.seq[sequenceName]
	sequence.seq[sequenceName]++
	return result, nil
}

func newPositionValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &positionValueProvider{}
	return result
}
