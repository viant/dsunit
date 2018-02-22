package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)


type datasetDmlProvider struct {
	*dsc.DmlBuilder
}

func (p *datasetDmlProvider) record(instance interface{}) *map[string]interface{} {
	switch result := instance.(type) {
	case map[string]interface{}:
		return &result
	case *map[string]interface{}:
		return result
	}
	return nil
}

func (p *datasetDmlProvider) Key(instance interface{}) []interface{} {
	record := instance.(map[string]interface{})
	var result = make([]interface{}, 0)
	for _, column := range p.TableDescriptor.PkColumns {
		var value = record[column]
		if toolbox.IsFloat(value) {
			value = toolbox.AsInt(value)
		}
		result = append(result, value)
	}
	return result
}

func (p *datasetDmlProvider) SetKey(instance interface{}, seq int64) {
	record := p.record(instance)
	key := p.TableDescriptor.PkColumns[0]
	(*record)[key] = seq
}

func (p *datasetDmlProvider) Get(sqlType int, instance interface{}) *dsc.ParametrizedSQL {
	record := p.record(instance)
	return p.GetParametrizedSQL(sqlType, func(column string) interface{} {
		return (*record)[column]
	})
}

func newDatasetDmlProvider(dmlBuilder *dsc.DmlBuilder) *datasetDmlProvider {
	return &datasetDmlProvider{dmlBuilder}
}

type datasetRowMapper struct {
	columns          []string
	columnToIndexMap map[string]int
}

func (m *datasetRowMapper) Map(scanner dsc.Scanner) (interface{}, error) {
	columnValues, columns, err := dsc.ScanRow(scanner)
	if err != nil {
		return nil, err
	}
	var values = make(map[string]interface{})
	for i, item := range columnValues {
		values[columns[i]] = item
	}
	return &values, nil
}

func newDatasetRowMapper(columns []string) dsc.RecordMapper {
	var columnToIndexMap = make(map[string]int)
	for index, column := range columns {
		columnToIndexMap[column] = index
	}
	var result dsc.RecordMapper = &datasetRowMapper{
		columns:          columns,
		columnToIndexMap: columnToIndexMap,
	}
	return result
}
