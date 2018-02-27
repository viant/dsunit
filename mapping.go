package dsunit

import "github.com/viant/toolbox/url"

//Mapping represents mapping
type Mapping struct {
	*url.Resource
	*MappingTable
	Name string `required:"true" description:"mapping name (i.e view name)"`
}

//MappingTable represents a table mapping, mapping allow to route data defined in only one table to many tables.
type MappingTable struct {
	Table   string           `required:"true"`
	Columns []*MappingColumn `required:"true"`

	Associations []*MappingTable
}

//MappingColumn represents column with its source definition.
type MappingColumn struct {
	Name         string `required:"true" description:"column name"`
	DefaultValue string
	FromColumn   string `description:"if specified it defined value source for this column"`
	Required     bool   `description:"table record will be mapped if values for all required columns are present"`
	Unique       bool   `description:"flag key/s that are unique"`
}

//Tables returns tables of this mapping
func (m *Mapping) Tables() []string {
	var result = make([]string, 0)
	addTables(&result, m.MappingTable)
	return result
}

func addTables(tables *[]string, mapping *MappingTable) {
	if mapping == nil {
		return
	}
	*tables = append(*tables, mapping.Table)
	if len(mapping.Associations) == 0 {
		return
	}
	for _, association := range mapping.Associations {
		addTables(tables, association)
	}
}
