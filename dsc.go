package dsunit

import (
	"github.com/pkg/errors"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"strings"
	"time"
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
	dsc.Scanner
	valueProviders   []func(slice []interface{}, index int)
	columns          []string
	columnToIndexMap map[string]int
}

func (m *datasetRowMapper) Map(scanner dsc.Scanner) (interface{}, error) {
	m.Scanner = scanner
	columnValues, columns, err := dsc.ScanRow(m)
	if err != nil {
		return nil, err
	}
	var values = make(map[string]interface{})
	for i, item := range columnValues {
		values[columns[i]] = item
	}
	return &values, nil
}

func (m *datasetRowMapper) buildProviders(types map[string]dsc.Column) []func(slice []interface{}, index int) {
	valueProvider := []func(slice []interface{}, index int){}

	if len(types) == 0 || len(m.columns) == 0 {
		return valueProvider
	}

	for _, column := range m.columns {
		if info, ok := types[column]; ok {
			dbTypeName := info.DatabaseTypeName()
			switch strings.ToUpper(dbTypeName) {
			case "VARCHAR", "VARCHAR2", "CHAR", "STRING", "TEXT":
				valueProvider = append(valueProvider, func(slice []interface{}, index int) {
					var value *string
					slice[index] = &value
				})
			case "DATE", "DATETIME", "TIMESTAMP":
				valueProvider = append(valueProvider, func(slice []interface{}, index int) {
					var value *time.Time
					slice[index] = &value
				})
			case "INT", "INTEGER", "BIGINT", "TINYINT", "INT64":
				valueProvider = append(valueProvider, func(slice []interface{}, index int) {
					var value *int
					slice[index] = &value
				})
			case "FLOAT", "FLOAT64", "DECIMAL", "NUMERIC":
				valueProvider = append(valueProvider, func(slice []interface{}, index int) {
					var value *float64
					slice[index] = &value
				})
			default:
				valueProvider = append(valueProvider, func(slice []interface{}, index int) {
					var value interface{}
					slice[index] = &value
				})
			}
		}
	}
	return valueProvider
}

func (m *datasetRowMapper) ColumnValues() ([]interface{}, error) {
	if len(m.valueProviders) == 0 {
		return nil, errors.New("column values not supported")
	}
	var result = make([]interface{}, len(m.columns))
	for i, provider := range m.valueProviders {
		provider(result, i)
	}
	return result, nil
}

func newDatasetRowMapper(columns []string, types []dsc.Column) dsc.RecordMapper {
	var columnToIndexMap = make(map[string]int)
	for index, column := range columns {
		columnToIndexMap[column] = index
	}
	var result = &datasetRowMapper{
		columns:          columns,
		columnToIndexMap: columnToIndexMap,
	}
	if len(types) > 0 {
		indexTypes := make(map[string]dsc.Column)
		for _, column := range types {
			indexTypes[column.Name()] = column
		}
		result.valueProviders = result.buildProviders(indexTypes)
	}
	return result
}
