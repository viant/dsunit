package dsunit

import (
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
)

type sequence struct {
	seq map[string]int64
}

type sequenceValueProvider struct {
	match string
}

func (p *sequenceValueProvider) countMatched(dataset *Dataset) int64 {
	var result = 0
	var keyColumn = ""
	pkColumns := dataset.Records.UniqueKeys()
	if len(pkColumns) > 0 {
		keyColumn = pkColumns[0]
	}
	for _, record := range dataset.Records {
		for k, v := range record {
			if keyColumn != "" && strings.ToUpper(keyColumn) != strings.ToUpper(k) {
				continue
			}
			if value, ok := v.(string); ok {
				if strings.Contains(value, p.match) {
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
	datastoreDialect := *context.GetOptional((*dsc.DatastoreDialect)(nil)).(*dsc.DatastoreDialect)
	seq, err := datastoreDialect.GetSequence(manager, sequenceName)
	if err != nil {
		return 0, err
	}
	insertableCount := p.countMatched(dataset)
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

func newSequenceValueProvider(exprMatch string) toolbox.ValueProvider {
	var result toolbox.ValueProvider = &sequenceValueProvider{match: exprMatch}
	return result
}

type queryValueProvider struct{}

func (p *queryValueProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	manager := *context.GetOptional((*dsc.Manager)(nil)).(*dsc.Manager)
	sql := toolbox.AsString(arguments[0])
	var row = make([]interface{}, 0)
	success, err := manager.ReadSingle(&row, sql, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to evalue macro with sql: %v, %v", sql, err)
	}
	if !success {
		return nil, nil
	}
	return row[0], nil
}

func newQueryValueProvider() toolbox.ValueProvider {
	return &queryValueProvider{}
}
