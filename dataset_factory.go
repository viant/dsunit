package dsunit

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"path"
)

type datasetFactoryImpl struct {
	managerRegistry dsc.ManagerRegistry
}

func (f datasetFactoryImpl) ensurePkValues(data map[string]interface{}, descriptor *dsc.TableDescriptor) {
	if descriptor == nil {
		return
	}
	for _, pkColumn := range descriptor.PkColumns {
		_, ok := data[pkColumn]
		if !ok {
			data[pkColumn] = nil
		}
	}
}

func (f datasetFactoryImpl) buildDatasetForRows(descriptor *dsc.TableDescriptor, rows []*Row) *Dataset {
	var allColumns = make(map[string]interface{})
	for i, row := range rows {
		if len(row.Values) > 0 {
			f.ensurePkValues(row.Values, descriptor)
			for key := range row.Values {
				allColumns[key] = true
			}
		}
		rows[i] = row
	}

	columns := toolbox.MapKeysToStringSlice(allColumns)
	return &Dataset{
		TableDescriptor: &dsc.TableDescriptor{
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
	manager := f.managerRegistry.Get(datastore)
	descriptor := manager.TableDescriptorRegistry().Get(table)
	return f.Create(descriptor, dataset...)
}

func (f datasetFactoryImpl) Create(descriptor *dsc.TableDescriptor, dataset ...map[string]interface{}) *Dataset {
	var rows = make([]*Row, len(dataset))
	for i, values := range dataset {

		if len(values) > 0 {
			f.ensurePkValues(values, descriptor)
		}

		var row = &Row{
			Source: fmt.Sprintf("Map, table:%v; row:%v", descriptor.Table, i),
			Values: values,
		}

		rows[i] = row
	}

	return f.buildDatasetForRows(descriptor, rows)
}

func (f datasetFactoryImpl) buildDataSetFromColumnarData(descriptor *dsc.TableDescriptor, url string, columns []string, dataset [][]interface{}) *Dataset {
	var rows = make([]*Row, len(dataset))
	for i, data := range dataset {
		var values = make(map[string]interface{})
		for i, item := range data {
			values[columns[i]] = item
		}
		var row = &Row{
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
		return nil, fmt.Errorf("failed to build dataset from %v due to: %v", url, err)
	}
	var rows = make([]*Row, len(transfer))
	for i, values := range transfer {
		if len(values) >0 {
			f.ensurePkValues(values, descriptor)
		}
		var row = &Row{
			Source: fmt.Sprintf("URI:%v, table:%v, line:%v", url, descriptor.Table, i),
			Values: values,
		}
		rows[i] = row
	}
	return f.buildDatasetForRows(descriptor, rows), nil
}

func (f datasetFactoryImpl) CreateFromURL(datastore string, table string, url string) (*Dataset, error) {
	reader, mimeType, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return f.createDataset(reader, datastore, table, mimeType, url)
}

func (f datasetFactoryImpl) createDataset(reader io.Reader, datastore, table, mimeType, url string) (*Dataset, error) {
	manager := f.managerRegistry.Get(datastore)
	descriptor := manager.TableDescriptorRegistry().Get(table)
	if descriptor == nil {
		descriptor = &dsc.TableDescriptor{
			Table: table,
		}
	}
	if mimeType == "text/csv" {
		columns, rows := ParseColumnarData(reader, ",")
		return f.buildDataSetFromColumnarData(descriptor, url, columns, rows), nil
	}
	if mimeType == "text/tsv" {
		columns, rows := ParseColumnarData(reader, "\t")
		return f.buildDataSetFromColumnarData(descriptor, url, columns, rows), nil
	}
	if strings.Contains(mimeType, "text/json") {
		return f.buildDatasetFromJSON(descriptor, url, reader)
	}

	return nil, dsUnitError{"Unsupprted mime type: " + mimeType + " on " + url}
}

//CreateDatasets crate a datasets from passed in data resources
func (f datasetFactoryImpl) CreateDatasets(dataResource *DatasetResource) (*Datasets, error) {
	var datasets = make([]*Dataset, 0)
	if dataResource.TableRows != nil {
		for _, rows := range dataResource.TableRows {
			dataset := f.CreateFromMap(dataResource.Datastore, rows.Table, rows.Rows...)
			datasets = append(datasets, dataset)
		}
	} else if dataResource.URL != "" {
		service, err := storage.NewServiceForURL(dataResource.URL, dataResource.Credential)
		if err != nil {
			return nil, err
		}
		defer service.Close()
		candidates, err := service.List(dataResource.URL)
		if err != nil {
			return nil, err
		}

		for _, cadidate := range candidates {
			if cadidate.IsFolder() {
				continue
			}
			_, name := path.Split(cadidate.URL())
			if dataResource.Prefix != "" {
				if !strings.HasPrefix(name, dataResource.Prefix) {
					continue
				}
				name = string(name[len(dataResource.Prefix):])
			}
			if dataResource.Postfix != "" {
				if !strings.HasSuffix(name, dataResource.Postfix) {
					continue
				}
				name = string(name[:len(name)-len(dataResource.Postfix)])
			}
			if strings.Index(name, ".") != -1 {
				name = string(name[:strings.Index(name, ".")])
			}
			reader, err := service.Download(cadidate)
			if err != nil {
				return nil, err
			}
			defer reader.Close()
			ext := path.Ext(cadidate.URL())
			if ext != "" {
				ext = string(ext[1:])
			}
			data, err := f.createDataset(reader, dataResource.Datastore, name, "text/"+ext, cadidate.URL())
			if err != nil {
				return nil, err
			}
			datasets = append(datasets, data)
		}
	}
	return &Datasets{
		Datastore: dataResource.Datastore,
		Datasets:  datasets,
	}, nil
}

func newDatasetFactory(managerRegistry dsc.ManagerRegistry) DatasetFactory {
	var datasetFactory DatasetFactory = &datasetFactoryImpl{managerRegistry: managerRegistry}
	return datasetFactory
}
