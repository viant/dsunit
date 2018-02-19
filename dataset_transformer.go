package dsunit
//
//import (
//	"github.com/viant/dsc"
//	"github.com/viant/toolbox"
//)
//
////DatasetTransformer represents a dataset transformer.
//type DatasetTransformer struct{}
//
//func getDataset(datasets map[string]*Dataset, table string, registry dsc.TableDescriptorRegistry, tables *[]string) *Dataset {
//	if result, found := datasets[table]; found {
//		return result
//	}
//	descriptor := registry.Get(table)
//	result := &Dataset{
//		TableDescriptor: descriptor,
//		Rows:            make([]*Record, 0),
//	}
//	datasets[table] = result
//	(*tables) = append((*tables), table)
//	return result
//}
//
//func (dt *DatasetTransformer) mapRowToDataset(row *Record, mapping *DatasetMapping) map[string]interface{} {
//	var values = make(map[string]interface{})
//	for _, column := range mapping.Columns {
//		fromColumn := column.FromColumn
//		if fromColumn == "" {
//			fromColumn = column.Name
//		}
//		if rowValue, found := row.Values[fromColumn]; found {
//			values[column.Name] = rowValue
//		} else {
//			if column.DefaultValue != "" {
//				values[column.Name] = column.DefaultValue
//			} else if column.Required {
//				return nil
//			}
//
//		}
//	}
//	return values
//}
//
//func (dt *DatasetTransformer) isRowDuplicated(dataset *Dataset, mapping *DatasetMapping, row *Record, tablesPk map[string]bool) bool {
//	if !dataset.Autoincrement {
//		var required = ""
//		for _, column := range mapping.Columns {
//			if column.Required {
//				required += column.Name
//			}
//		}
//		pkValue := "__ " + mapping.Table + required + ":"
//		var hasPk = false
//		for _, pkColumn := range dataset.PkColumns {
//			if value, ok := row.Values[pkColumn]; ok {
//				pkValue += toolbox.AsString(value) + ":"
//				hasPk = true
//			}
//		}
//		if !hasPk {
//			return false
//		}
//
//		if _, found := tablesPk[pkValue]; found {
//			return true
//		}
//		tablesPk[pkValue] = true
//	}
//	return false
//}
//
//func (dt *DatasetTransformer) mapRowToDatasets(row *Record, mapping *DatasetMapping, datasets map[string]*Dataset, registry dsc.TableDescriptorRegistry, tables *[]string, tablesPk map[string]bool) {
//	values := dt.mapRowToDataset(row, mapping)
//	if values != nil {
//		dataset := getDataset(datasets, mapping.Table, registry, tables)
//		mappedRow := &Record{Source: row.Source, Values: values}
//
//		if dt.isRowDuplicated(dataset, mapping, mappedRow, tablesPk) {
//			return
//		}
//		dataset.Rows = append(dataset.Rows, mappedRow)
//	}
//	if len(mapping.Associations) > 0 {
//		for _, association := range mapping.Associations {
//			dt.mapRowToDatasets(row, association, datasets, registry, tables, tablesPk)
//		}
//	}
//}
//
////Transform routes source dataset data into mapping dataset, it uses source dataset and table descriptor registry to build resulting datasets.
//func (dt *DatasetTransformer) Transform(datastore string, sourceDataset *Dataset, mapping *DatasetMapping, registry dsc.TableDescriptorRegistry) *DatastoreDatasets {
//	var datasets = make(map[string]*Dataset)
//	var tables = make([]string, 0)
//	var tablesPk = make(map[string]bool)
//
//	for _, row := range sourceDataset.Rows {
//		dt.mapRowToDatasets(row, mapping, datasets, registry, &tables, tablesPk)
//	}
//	result := &DatastoreDatasets{
//		Datastore: datastore,
//		DatastoreDatasets:  make([]*Dataset, 0),
//	}
//
//	for _, table := range tables {
//		value := datasets[table]
//		if value == nil || (*value).Rows == nil || len((*value).Rows) == 0 {
//			continue
//		}
//		result.DatastoreDatasets = append(result.DatastoreDatasets, value)
//	}
//	return result
//}
//
////NewDatasetTransformer returns a new NewDatasetTransformer
//func NewDatasetTransformer() *DatasetTransformer {
//	return &DatasetTransformer{}
//}
//
//type datasetTransformerRegistry struct {
//	registry map[string]*DatasetMapping
//}
//
////Register register dataset mapping with name
//func (r *datasetTransformerRegistry) register(name string, mapping *DatasetMapping) {
//	r.registry[name] = mapping
//}
//
////Get returns dataset mapping for passed in name.
//func (r *datasetTransformerRegistry) get(name string) *DatasetMapping {
//	if result, ok := r.registry[name]; ok {
//		return result
//	}
//	return nil
//}
//
////Has returns true if data mapping has been registered.
//func (r *datasetTransformerRegistry) has(name string) bool {
//	if _, ok := r.registry[name]; ok {
//		return true
//	}
//	return false
//}
//
////Names returns names of registered dataset mapping.
//func (r *datasetTransformerRegistry) names() []string {
//	return toolbox.MapKeysToStringSlice(r.registry)
//}
//
////NewDatasetTransformerRegistry returns new NewDatasetTransformerRegistry
//func newDatasetTransformerRegistry() *datasetTransformerRegistry {
//	return &datasetTransformerRegistry{
//		registry: make(map[string]*DatasetMapping),
//	}
//}
