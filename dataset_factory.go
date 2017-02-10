package dsunit

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type datasetFactoryImpl struct {
	managerRegistry dsc.ManagerRegistry
}

func (f datasetFactoryImpl) ensurePkValues(data map[string]interface{}, descriptor *dsc.TableDescriptor) {
	for _, pkColumn := range descriptor.PkColumns {
		_, ok := data[pkColumn]
		if !ok {
			data[pkColumn] = nil
		}
	}
}

func (f datasetFactoryImpl) buildDatasetForRows(descriptor *dsc.TableDescriptor, rows []Row) *Dataset {
	var allColumns = make(map[string]interface{})
	for i, row := range rows {
		f.ensurePkValues(row.Values, descriptor)
		for key := range row.Values {
			allColumns[key] = true
		}
		rows[i] = row
	}
	columns := toolbox.MapKeysToStringSlice(allColumns)
	return &Dataset{
		TableDescriptor: dsc.TableDescriptor{
			Table:         descriptor.Table,
			PkColumns:     descriptor.PkColumns,
			Autoincrement: descriptor.Autoincrement,
			Columns:       columns,
			FromQuery:     descriptor.FromQuery,
		},
		Rows: rows,
	}
}

func (f datasetFactoryImpl) CreateFromMap(datastore string, table string, dataset ...map[string]interface{}) *Dataset {
	descriptor := f.getDescriptor(datastore, table)
	return f.Create(descriptor, dataset...)
}

func (f datasetFactoryImpl) Create(descriptor *dsc.TableDescriptor, dataset ...map[string]interface{}) *Dataset {
	var rows = make([]Row, len(dataset))
	for i, values := range dataset {
		f.ensurePkValues(values, descriptor)
		var row = Row{
			Source: fmt.Sprintf("Map, table:%v; row:%v", descriptor.Table, i),
			Values: values,
		}
		rows[i] = row
	}

	return f.buildDatasetForRows(descriptor, rows)
}

func (f datasetFactoryImpl) buildDataSetFromColumnarData(descriptor *dsc.TableDescriptor, url string, columns []string, dataset [][]interface{}) *Dataset {
	var rows = make([]Row, len(dataset))
	for i, data := range dataset {
		var values = make(map[string]interface{})
		for i, item := range data {
			values[columns[i]] = item
		}
		var row = Row{
			Source: fmt.Sprintf("URI:%v, table:%v, line:%v", url, descriptor.Table, i),
			Values: values,
		}
		rows[i] = row
	}
	return f.buildDatasetForRows(descriptor, rows)
}

func (f datasetFactoryImpl) buildDatasetFromJSON(descriptor *dsc.TableDescriptor, url string, reader io.Reader) (*Dataset, error) {
	var transfer = make([]map[string]interface{}, 0)
	err := json.NewDecoder(reader).Decode(&transfer)
	if err != nil {
		return nil, fmt.Errorf("Failed to build dataset from %v due to: %v", url, err)
	}
	var rows = make([]Row, len(transfer))
	for i, values := range transfer {
		f.ensurePkValues(values, descriptor)
		var row = Row{
			Source: fmt.Sprintf("URI:%v, table:%v, line:%v", url, descriptor.Table, i),
			Values: values,
		}
		rows[i] = row
	}
	return f.buildDatasetForRows(descriptor, rows), nil
}

func (f datasetFactoryImpl) getDescriptor(datastore string, table string) *dsc.TableDescriptor {
	mnager := f.managerRegistry.Get(datastore)
	descriptor := mnager.TableDescriptorRegistry().Get(table)
	return descriptor
}

func (f datasetFactoryImpl) CreateFromURL(datastore string, table string, url string) (*Dataset, error) {
	descriptor := f.getDescriptor(datastore, table)
	reader, mimeType, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	if mimeType == "text/csv" {
		columns, rows := parseColumnarData(reader, ",")
		return f.buildDataSetFromColumnarData(descriptor, url, columns, rows), nil
	}
	if mimeType == "text/tsv" {
		columns, rows := parseColumnarData(reader, "\t")
		return f.buildDataSetFromColumnarData(descriptor, url, columns, rows), nil
	}
	if strings.Contains(mimeType, "text/json") {
		return f.buildDatasetFromJSON(descriptor, url, reader)
	}

	return nil, dsUnitError{"Unsupprted mime type: " + mimeType + " on " + url}
}

func newDatasetFactory(managerRegistry dsc.ManagerRegistry) DatasetFactory {
	var datasetFactory DatasetFactory = &datasetFactoryImpl{managerRegistry: managerRegistry}
	return datasetFactory
}
