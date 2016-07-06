package dsunit

import (
	"fmt"
	"reflect"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

//AsRow cast instance as Row.
func AsRow(instance interface{}) *Row {
	if result, ok := instance.(Row); ok {
		return &result
	}
	if result, ok := instance.(*Row); ok {
		return result
	}
	panic(fmt.Sprintf("Instance should be a Row type, but had: %T", instance))
}

//Value returns raw column value for this row.
func (r Row) Value(column string) interface{} {
	if value, ok := r.Values[column]; ok {
		return value
	}
	return nil
}

//ValueAsString returns column value as string.
func (r Row) ValueAsString(column string) string {
	value := r.Values[column]
	return fmt.Sprintf("%v", value)
}

//Columns returns column names.
func (r Row) Columns() []string {
	return toolbox.MapKeysToStringSlice(r.Values)
}

//SetValue sets column value on this row.
func (r Row) SetValue(column string, value interface{}) {
	r.Values[column] = value
}

//HasColumn returns true if this row has passed in column value.
func (r Row) HasColumn(column string) bool {
	if _, ok := r.Values[column]; ok {
		return true
	}
	return false
}

//String prints row content.
func (r Row) String() string {
	result := ""
	var sortedColumns = toolbox.SortStrings(r.Columns())
	for _, column := range sortedColumns {
		if len(result) > 0 {
			result = result + ", "
		}

		valueType := reflect.ValueOf(r.Values[column])
		if valueType.Kind() == reflect.String {
			result = result + column + ":\"" + r.ValueAsString(column) + "\""
		} else {
			result = result + column + ":" + r.ValueAsString(column)
		}
	}
	return "{" + result + "}"
}

func readValues(instance interface{}, columns []string) []interface{} {
	row := AsRow(instance)
	var result = make([]interface{}, len(columns))
	for i, column := range columns {
		result[i] = readValue(row, column)
	}
	return result
}

func readValue(row *Row, column string) interface{} {
	return (*row).Value(column)
}

type datasetDmlProvider struct {
	dmlBuilder *dsc.DmlBuilder
}

func (p *datasetDmlProvider) PkColumns() []string {
	return p.dmlBuilder.TableDescriptor.PkColumns
}

func (p *datasetDmlProvider) Key(instance interface{}) []interface{} {
	result := readValues(instance, p.dmlBuilder.TableDescriptor.PkColumns)
	return result
}

func (p *datasetDmlProvider) SetKey(instance interface{}, seq int64) {
	key := p.dmlBuilder.TableDescriptor.PkColumns[0]
	row := AsRow(instance)
	(*row).SetValue(key, seq)
}

func (p *datasetDmlProvider) Get(sqlType int, instance interface{}) *dsc.ParametrizedSQL {
	row := AsRow(instance)
	return p.dmlBuilder.GetParametrizedSQL(sqlType, func(column string) interface{} {
		return readValue(row, column)
	})
}

func newDatasetDmlProvider(dmlBuilder *dsc.DmlBuilder) dsc.DmlProvider {
	var result dsc.DmlProvider = &datasetDmlProvider{dmlBuilder}
	return result
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
	var result = Row{
		Source: "DatasetRowMapper",
		Values: values,
	}
	return &result, nil

}

func newDatasetRowMapper(columns []string, columnToIndexMap map[string]int) dsc.RecordMapper {
	if columnToIndexMap == nil {
		index := 0
		columnToIndexMap = make(map[string]int)
		toolbox.SliceToMap(columns, columnToIndexMap, toolbox.CopyStringValueProvider, func(column string) int {
			index++
			return index
		})
	}

	var result dsc.RecordMapper = &datasetRowMapper{
		columns:          columns,
		columnToIndexMap: columnToIndexMap,
	}
	return result
}
