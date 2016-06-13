/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */
package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

//DatasetTransformer represents a dataset transformer.
type DatasetTransformer struct{}

func getDataset(datasets map[string]*Dataset, table string, registry dsc.TableDescriptorRegistry, tables *[]string) *Dataset {
	if result, found := datasets[table]; found {
		return result
	}
	result := &Dataset{
		TableDescriptor: *registry.Get(table),
		Rows:            make([]Row, 0),
	}
	datasets[table] = result
	(*tables) = append((*tables), table)
	return result
}

func (dt *DatasetTransformer) mapRowToDataset(row *Row, mapping *DatasetMapping) map[string]interface{} {
	var values = make(map[string]interface{})
	for _, column := range mapping.Columns {
		fromColumn := column.FromColumn
		if fromColumn == "" {
			fromColumn = column.Name
		}
		if rowValue, found := row.Values[fromColumn]; found {
			values[column.Name] = rowValue
		} else {
			if column.DefaultValue != "" {
				values[column.Name] = column.DefaultValue
			} else if column.Required {
				return nil
			}

		}
	}
	return values
}

func (dt *DatasetTransformer) isRowDuplicated(dataset *Dataset, mapping *DatasetMapping, row *Row, tablesPk map[string]bool) bool {
	if !dataset.Autoincrement {
		pkValue := "__ " + mapping.Table + ":"
		for _, pkColumn := range dataset.PkColumns {
			if value, ok := row.Values[pkColumn]; ok {
				pkValue += toolbox.AsString(value) + ":"
			}
		}
		if _, found := tablesPk[pkValue]; found {
			return true
		}
		tablesPk[pkValue] = true
	}
	return false
}

func (dt *DatasetTransformer) mapRowToDatasets(row *Row, mapping *DatasetMapping, datasets map[string]*Dataset, registry dsc.TableDescriptorRegistry, tables *[]string, tablesPk map[string]bool) {
	values := dt.mapRowToDataset(row, mapping)
	if values != nil {
		dataset := getDataset(datasets, mapping.Table, registry, tables)
		mappedRow := Row{Source: row.Source, Values: values}
		if dt.isRowDuplicated(dataset, mapping, &mappedRow, tablesPk) {
			return
		}
		dataset.Rows = append(dataset.Rows, mappedRow)
	}
	if mapping.Associations != nil {
		for _, association := range mapping.Associations {
			dt.mapRowToDatasets(row, &association, datasets, registry, tables, tablesPk)
		}
	}
}

//Transform routes source dataset data into mapping dataset, it uses source dataset and table descriptor registry to build resulting datasets.
func (dt *DatasetTransformer) Transform(datastore string, sourceDataset *Dataset, mapping *DatasetMapping, registry dsc.TableDescriptorRegistry) *Datasets {
	var datasets = make(map[string]*Dataset)
	var tables = make([]string, 0)
	var tablesPk = make(map[string]bool)
	for _, row := range sourceDataset.Rows {
		dt.mapRowToDatasets(&row, mapping, datasets, registry, &tables, tablesPk)
	}
	result := &Datasets{
		Datastore: datastore,
		Datasets:  make([]Dataset, 0),
	}

	for _, table := range tables {
		value := datasets[table]
		if value == nil || (*value).Rows == nil || len((*value).Rows) == 0 {
			continue
		}
		result.Datasets = append(result.Datasets, *value)
	}
	return result
}

//NewDatasetTransformer returns a new NewDatasetTransformer
func NewDatasetTransformer() *DatasetTransformer {
	return &DatasetTransformer{}
}

type datasetTransformerRegistry struct {
	registry map[string]*DatasetMapping
}

//Register register dataset mapping with name
func (r *datasetTransformerRegistry) register(name string, mapping *DatasetMapping) {
	r.registry[name] = mapping
}

//Get returns dataset mapping for passed in name.
func (r *datasetTransformerRegistry) get(name string) *DatasetMapping {
	if result, ok := r.registry[name]; ok {
		return result
	}
	return nil
}

//Has returns true if data mapping has been registered.
func (r *datasetTransformerRegistry) has(name string) bool {
	if _, ok := r.registry[name]; ok {
		return true
	}
	return false
}

//Names returns names of registered dataset mapping.
func (r *datasetTransformerRegistry) names() []string {
	return toolbox.MapKeysToStringSlice(r.registry)
}

//NewDatasetTransformerRegistry returns new NewDatasetTransformerRegistry
func newDatasetTransformerRegistry() *datasetTransformerRegistry {
	return &datasetTransformerRegistry{
		registry: make(map[string]*DatasetMapping),
	}
}
