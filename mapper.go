package dsunit

import (
	"github.com/viant/toolbox"
)

type Mapper struct {
	mappings map[string]*Mapping
}

func (s *Mapper) transform(table *MappingTable, virtualRecord map[string]interface{}, datasets map[string]*Dataset, tableKeys map[string]map[string]interface{}) {
	for _, association := range table.Associations {
		s.transform(association, virtualRecord, datasets, tableKeys)
	}
	var record = make(map[string]interface{})
	var uniqueKey = table.Table + "/"
	for _, column := range table.Columns {
		fromColumn := column.FromColumn
		if fromColumn == "" {
			fromColumn = column.Name
		}
		var rowValue interface{}
		var found bool
		if rowValue, found = virtualRecord[fromColumn]; !found && column.DefaultValue != "" {
			rowValue = column.DefaultValue
		}

		if column.Unique {
			if rowValue == nil {
				return
			}
			uniqueKey += toolbox.AsString(rowValue)
		}
		if rowValue == nil {
			continue
		}
		record[column.Name] = rowValue
	}
	if _, has := datasets[table.Table]; !has {
		datasets[table.Table] = NewDataset(table.Table)
	}
	if existingRecord, has := tableKeys[uniqueKey]; has {
		for k, v := range record {
			if _, has := existingRecord[k]; !has {
				existingRecord[k] = v

			}
		}
		return
	}
	tableKeys[uniqueKey] = record
	datasets[table.Table].Records = append(datasets[table.Table].Records, record)
	if len(table.Associations) == 0 {
		return
	}

}

func (s *Mapper) Add(mapping *Mapping) {
	s.mappings[mapping.Name] = mapping
}

func (s *Mapper) Has(table string) bool {
	_, ok := s.mappings[table]
	return ok
}

func (s *Mapper) Map(dataset *Dataset) []*Dataset {
	var namedDatasets = make(map[string]*Dataset)
	var tableKeys = make(map[string]map[string]interface{})

	mapping, ok := s.mappings[dataset.Table]
	if !ok {
		return nil
	}

	for _, record := range dataset.Records {
		s.transform(mapping.MappingTable, record, namedDatasets, tableKeys)
	}
	var result = make([]*Dataset, 0)
	for _, table := range mapping.Tables() {
		if dataset, has := namedDatasets[table]; has {
			result = append(result, dataset)
		}
	}
	return result
}

func NewMapper() *Mapper {
	return &Mapper{
		mappings: make(map[string]*Mapping),
	}
}
