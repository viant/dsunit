package dsunit

import (
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

const selectSql = "select row_number() over(order by %v) as position, %v from %v"

type rowNumberBasedPositionProvider struct{}

func (p *rowNumberBasedPositionProvider) filterColumns(columns []string) []string {
	var filteredCols []string
	for _, column := range columns {
		if column != "position" {
			filteredCols = append(filteredCols, column)
		}

	}
	return filteredCols
}

func (p *rowNumberBasedPositionProvider) Get(context toolbox.Context, arguments ...interface{}) (interface{}, error) {
	tableName := toolbox.AsString(arguments[0])
	dataset := context.GetOptional((*Dataset)(nil)).(*Dataset)
	result := fmt.Sprintf(selectSql, strings.Join(dataset.OrderColumns, ", "), strings.Join(p.filterColumns(dataset.Columns), ", "), tableName)
	return result, nil
}

func newBgQueryProvider() toolbox.ValueProvider {
	var result toolbox.ValueProvider = &rowNumberBasedPositionProvider{}
	return result
}
