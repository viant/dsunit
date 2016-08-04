package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

type sequence struct {
	seq map[string]int64
}

type sequenceValueProvider struct{}

func (p *sequenceValueProvider) countInsertable(dataset *Dataset) int64 {
	var result = 0
	for _, row := range dataset.Rows {
		for _, pkColumn := range dataset.PkColumns {
			value := row.Value(pkColumn)
			if textValue, ok := value.(string); ok {
				if strings.Contains(textValue, ":seq ") {
					result++
				}
			}
		}
	}
	return int64(result)
}

func (p *sequenceValueProvider) fetchSequence(context toolbox.Context, sequenceName string) (int64, error) {
	manager := *context.GetOptional((*dsc.Manager)(nil)).(*dsc.Manager)
	dataset := context.GetOptional((*Dataset)(nil)).(*Dataset)
	sqlDialectable := *context.GetOptional((*dsc.DatastoreDialect)(nil)).(*dsc.DatastoreDialect)
	seq, err := sqlDialectable.GetSequence(manager, sequenceName)
	if err != nil {
		return 0, err
	}
	insertableCount := p.countInsertable(dataset)

	result := seq - insertableCount
	return result, nil
}

func (p *sequenceValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	sequenceName := toolbox.AsString(arguments[0])

	if !context.Contains((*sequence)(nil)) {
		seq, err := p.fetchSequence(context, sequenceName)
		if err != nil {
			return nil, err
		}
		var sequenceValue = sequence{seq: make(map[string]int64)}
		sequenceValue.seq[sequenceName] = seq
		context.Put((*sequence)(nil), &sequenceValue)
	}
	var sequence = context.GetOptional((*sequence)(nil)).(*sequence)
	result := sequence.seq[sequenceName]
	sequence.seq[sequenceName]++
	return result, nil
}

func newSequenceValueProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &sequenceValueProvider{}
	return result
}

type queryValueProvider struct{}

func (p *queryValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	manager := *context.GetOptional((*dsc.Manager)(nil)).(*dsc.Manager)
	sql := toolbox.AsString(arguments[0])
	var row = make([]interface{}, 0)
	success, err := manager.ReadSingle(&row, sql, nil, nil)
	if err != nil {
		return nil, dsUnitError{"Failed to evalue macro with sql: " + sql + " due to:\n\t" + err.Error()}
	}
	if !success {
		return nil, nil
	}
	return row[0], nil
}

func newQueryValueProvider() toolbox.ValueProvider {
	return &queryValueProvider{}
}
