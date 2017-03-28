package dsunit

import (
	"github.com/viant/toolbox"
	"fmt"
	"reflect"
	"strings"
	"bytes"
)

const mainSelectSql = "select row_number() over(order by %v) as position, %v from %v"

const subSelectSql  = "select row_number() over(order by %v) as position, %v, group_concat(unique(%v)) within record as %v from %v"

type bgQueryProviderLegacy struct{}

func (p *bgQueryProviderLegacy) readRowValues(rowValues map[string]interface{}, nestedFields map[string]interface{}, repeatedFields map[string]interface{}) (error) {
	for key, val:= range rowValues {
		if (key == "position") {
			continue
		}
		valueType := reflect.TypeOf(val).Kind()
		switch valueType {
		case reflect.Map:
			valueMap := val.(map[string]interface{})
			nestedMap := make(map[string]interface{}, 0)
			p.parseMap(valueMap, key, nestedMap, repeatedFields)
			nestedFields[key] = nestedMap
		case reflect.Slice:
			valueArr := val.([]interface{})
			p.parseArray(valueArr, key, key, nestedFields, repeatedFields)
			nestedFields[key] = strings.ToUpper(key) + "." + key
		default:
			nestedFields[key] = key
		}
	}
	return nil
}

func (p *bgQueryProviderLegacy) parseMap(aMap map[string]interface{}, path string, nestedFields map[string]interface{}, repeatedFields map[string]interface{}) (error) {
	parentPath := path
	for key, val:= range aMap {
		valueType := reflect.TypeOf(val).Kind()
		switch valueType {
			case reflect.Map:
				path = parentPath + "." + key
				valueMap := val.(map[string]interface{})
				nestedMap := make(map[string]interface{}, 0)
				p.parseMap(valueMap, path, nestedMap, repeatedFields)
				nestedFields[key] = nestedMap
			case reflect.Slice:
				valueArr := val.([]interface{})
				p.parseArray(valueArr, path, key, nestedFields, repeatedFields)
				nestedFields[path] = "[" + strings.ToUpper(parentPath) + "." + key + "]"
			default:
				path = parentPath + "." + key
				nestedFields[key] = path
		}
	}

	return nil
}

func (p *bgQueryProviderLegacy) parseArray(anArray []interface{}, path string, key string, nestedFields map[string]interface{}, repeatedFields map[string]interface{}) (error) {
	parentPath := path
	for _, val := range anArray {
		valueType := reflect.TypeOf(val).Kind()
		switch valueType {
			case reflect.Map:
				path = parentPath + "." + key
				valueMap := val.(map[string]interface{})
				nestedMap := make(map[string]interface{}, 0)
				p.parseMap(valueMap, path, nestedMap, repeatedFields)
				nestedFields[key] = nestedMap
			case reflect.Slice:
				valueArr := val.([]interface{})
				p.parseArray(valueArr, path, key, nestedFields, repeatedFields)
				nestedFields[path] = "[" + strings.ToUpper(parentPath) + "." + key + "]"
			default:
				repeatedFields[path] = "[" + path + "]"
		}
	}
	return nil
}

func (p *bgQueryProviderLegacy) buildFieldObject(valueMap map[string]interface{}) (string) {
	var fields []string
	for key, val := range valueMap {
		var buffer bytes.Buffer
		switch reflect.TypeOf(val).Kind() {
		case reflect.Map:
			objectMap := val.(map[string]interface{})
			buffer.WriteString("concat(\"" + key + ":{\", ")
			buffer.WriteString(p.buildFieldList(objectMap) + ", \"}\"")
			buffer.WriteString(")")
		default:
			buffer.WriteString("concat(\"" + key + ":\", ")
			buffer.WriteString(val.(string) + ")")
		}
		fields = append(fields, buffer.String())
	}
	result := strings.Join(fields, ", ")

	return result
}

func (p *bgQueryProviderLegacy) buildFieldList(aMap map[string]interface{}) (string) {
	var fields []string
	for key, val := range aMap {
		var buffer bytes.Buffer
		switch reflect.TypeOf(val).Kind() {
			case reflect.Map:
				valueMap := val.(map[string]interface{})
				buffer.WriteString("concat(\"" + key + ":{\", ")
				buffer.WriteString(p.buildFieldObject(valueMap) + ", \"}\"")
				buffer.WriteString(") as " + key)
			default:
				buffer.WriteString(val.(string))
		}
		fields = append(fields, buffer.String())
	}
	result := strings.Join(fields, ", ")

	return result
}

func (p *bgQueryProviderLegacy) buildMainSelectQuery(nestedFields map[string]interface{}, dataset *Dataset, tableName string) (string) {
	fmt.Println("building main select from nestedFields: ", nestedFields)
	expectedFields := p.buildFieldList(nestedFields)
	orderColumns := strings.Join(dataset.TableDescriptor.OrderColumns, ",")
	mainSelect := fmt.Sprintf(mainSelectSql, orderColumns, expectedFields, tableName)
	fmt.Println("mainSelect = ", mainSelect)
	return mainSelect
}

func (p *bgQueryProviderLegacy) filterColumns(columns []string) ([]string) {
	var filteredCols []string
	for _, column := range columns {
		if column != "position" {
			filteredCols = append(filteredCols, column)
		}

	}
	return filteredCols
}

func (p *bgQueryProviderLegacy) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	tableName := toolbox.AsString(arguments[0])
	dataset := context.GetOptional((*Dataset)(nil)).(*Dataset)


	result := "select row_number() over(order by " + strings.Join(dataset.OrderColumns, ", ") + ") as position, " + strings.Join(p.filterColumns(dataset.Columns), ", ") + " from " + tableName

	/*
	var nestedFields = make(map[string]interface{}, 0)
	var repeatedFields = make(map[string]interface{}, 0)
	dataset := context.GetOptional((*Dataset)(nil)).(*Dataset)

	for _, row := range dataset.Rows {
		p.readRowValues(row.Values, nestedFields, repeatedFields)
	}

	mainSelect := p.buildMainSelectQuery(nestedFields, dataset, tableName)
	result.WriteString(mainSelect)

	//add join tables for repeated fields
	for field, _ := range repeatedFields {
		joinedTable := strings.ToUpper(field)
		subSelect := fmt.Sprintf(subSelectSql, dataset.OrderColumns, field, tableName)
		result.WriteString(" Join (")
		result.WriteString(subSelect)
		result.WriteString(") ")
		result.WriteString(joinedTable)
		result.WriteString(" on ")

		joinConditions := make([]string, 0)
		for _, pkColumn := range dataset.PkColumns {
			joinConditions = append(joinConditions, joinedTable, ".", pkColumn, "=", tableName, "." + pkColumn)
		}
		result.WriteString(strings.Join(joinConditions, " and "))
	}
	dataset.PkColumns = append(dataset.PkColumns, "position")
	*/

	return result, nil
}

func newBgQueryProviderLegacy() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &bgQueryProviderLegacy{}
	return result
}

