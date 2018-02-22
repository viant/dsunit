package dsunit

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)


type Record map[string]interface{}

//Value returns raw column value for this row.
func (r *Record) Value(column string) interface{} {
	if value, ok := (*r)[column]; ok {
		return value
	}
	return nil
}

//ValueAsString returns column value as string.
func (r Record) ValueAsString(column string) string {
	return fmt.Sprintf("%v", r.Value(column))
}

//IsEmpty return true if empty
func (r *Record) IsEmpty() bool {
	return len(r.Columns()) ==0
}


//Columns returns column names.
func (r *Record) Columns() []string {
	var result = make([]string, 0)
	for k := range (*r) {
		if k == "" || strings.HasPrefix(k, "@") && strings.Count(k, "@")  > 1 {
			continue
		}
		result = append(result, k)
	}
	return result
}


//SetValue sets column value on this row.
func (r *Record) SetValue(column string, value interface{}) {
	(*r)[column] = value
}

//HasColumn returns true if this row has passed in column value.
func (r *Record) HasColumn(column string) bool {
	if _, ok := (*r)[column]; ok {
		return true
	}
	return false
}

//AsMAp returns record as map
func (r *Record) AsMap() map[string]interface{} {
	var result = make(map[string]interface{})
	for _, column := range r.Columns() {
		result[column] = r.Value(column)
	}
	return result
}

//String return row content as string JSON.
func (r *Record) String() string {
	if result, err := toolbox.AsJSONText(r.AsMap())	;err == nil {
		return result
	}
	return fmt.Sprintf("%v", (*r))
}

