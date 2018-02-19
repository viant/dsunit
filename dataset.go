package dsunit

import (
	"github.com/viant/toolbox/url"
	"github.com/viant/toolbox/storage"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"github.com/viant/toolbox"
	"encoding/json"
	"bytes"
	"github.com/viant/dsunit/sv"
)

//Records represent data records
type Dataset struct {
	Table   string  `required:"true"`
	Records Records `required:"true"`
}

func NewDataset(table string, records ... map[string]interface{}) *Dataset {
	return &Dataset{
		Table:table,
		Records:records,
	}
}

//Records represnet table records
type Records []map[string]interface{}


//DatastoreDatasets represents a collection of datastore datasets
type DatastoreDatasets struct {
	Datastore string `required:"true" description:"register datastore"`
	Datasets  []*Dataset
}

//DatasetResource represents a dataset resource
type DatasetResource struct {
	*url.Resource      ` description:"data file location, csv, json, ndjson formats are supported"`
	*DatastoreDatasets  `required:"true" description:"datastore datasets"`
	Prefix  string     ` description:"location data file prefix"`  //apply prefix
	Postfix string     ` description:"location data file postgix"` //apply suffix
}



//Loads datasets from specified resource
func (r *DatasetResource) Load() (err error) {

	if r.Resource == nil || r.Resource.URL == "" {
		err = errors.New("resource was emtpy")
		return err
	}
	var storageService storage.Service
	storageService, err = storage.NewServiceForURL(r.URL, r.Credential)
	if err != nil {
		return err
	}
	var candidates []storage.Object
	candidates, err = storageService.List(r.Resource.URL)
	if err != nil {
		return err
	}
	for _, candidate := range candidates {
		if candidate.FileInfo().IsDir() {
			continue
		}
		err = r.load(storageService, candidate)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *DatasetResource) load(service storage.Service, object storage.Object) (err error) {
	if len(r.Datasets) == 0 {
		r.Datasets = make([]*Dataset, 0)
	}
	datafile := NewDatafileInfo(object.FileInfo().Name(), r.Prefix, r.Postfix)
	if datafile == nil {
		return nil
	}
	var loader func(datafile *DatafileInfo, data []byte) error
	switch datafile.Ext {
	case "json":
		loader = r.loadJSON
	case "csv":
		loader = r.loadCSV
	case "tsv":
		loader = r.loadTSV
	}
	if loader != nil {
		var reader io.ReadCloser
		if reader, err = service.Download(object); err == nil {
			defer reader.Close()
			var content []byte
			if content, err = ioutil.ReadAll(reader); err == nil {
				err = loader(datafile, content)
			}
		}
	}
	return nil
}

func (r *DatasetResource) loadJSON(datafile *DatafileInfo, data []byte) error {
	var dataSet = &Dataset{
		Table:   datafile.Name,
		Records: make([]map[string]interface{}, 0),
	}
	if toolbox.IsNewLineDelimitedJSON(string(data)) {
		if records, err := toolbox.NewLineDelimitedJSON(string(data)); err == nil {
			for _, record := range records {
				if recordMap, ok := record.(map[string]interface{}); ok {
					dataSet.Records = append(dataSet.Records, recordMap)
				}
			}
		}
	}
	if err := json.NewDecoder(bytes.NewReader(data)).Decode(dataSet.Records); err != nil {
		return err
	}
	r.Datasets = append(r.Datasets, dataSet)
	return nil
}

func (r *DatasetResource) loadCSV(datafile *DatafileInfo, data []byte) error {
	return r.loadSeparatedData(",", datafile, data)
}

func (r *DatasetResource) loadTSV(datafile *DatafileInfo, data []byte) error {
	return r.loadSeparatedData("\t", datafile, data)
}

func (r *DatasetResource) loadSeparatedData(delimiter string, datafile *DatafileInfo, data []byte) error {
	records, err := sv.NewSeparatedValueParser(delimiter).Parse(data)
	if err != nil {
		return err
	}
	var dataSet = &Dataset{
		Table:   datafile.Name,
		Records: records,
	}
	r.Datasets = append(r.Datasets, dataSet)
	return nil
}






//
//
//type datasetDmlProvider struct {
//	dmlBuilder *dsc.DmlBuilder
//}
//
//func (p *datasetDmlProvider) Key(instance interface{}) []interface{} {
//	result := readValues(instance, p.dmlBuilder.TableDescriptor.PkColumns)
//	for i, value := range result {
//		if toolbox.IsFloat(value) {
//			result[i] = toolbox.AsInt(value)
//		}
//	}
//	return result
//}
//
//func (p *datasetDmlProvider) SetKey(instance interface{}, seq int64) {
//	key := p.dmlBuilder.TableDescriptor.PkColumns[0]
//	row := AsRow(instance)
//	(*row).SetValue(key, seq)
//}
//
//func (p *datasetDmlProvider) Get(sqlType int, instance interface{}) *dsc.ParametrizedSQL {
//	row := AsRow(instance)
//	return p.dmlBuilder.GetParametrizedSQL(sqlType, func(column string) interface{} {
//		return readValue(row, column)
//	})
//}
//
//func newDatasetDmlProvider(dmlBuilder *dsc.DmlBuilder) dsc.DmlProvider {
//	var result dsc.DmlProvider = &datasetDmlProvider{dmlBuilder}
//	return result
//}
//
//type datasetRowMapper struct {
//	columns          []string
//	columnToIndexMap map[string]int
//}
//
//func (m *datasetRowMapper) Map(scanner dsc.Scanner) (interface{}, error) {
//	columnValues, columns, err := dsc.ScanRow(scanner)
//	if err != nil {
//		return nil, err
//	}
//	var values = make(map[string]interface{})
//	for i, item := range columnValues {
//		values[columns[i]] = item
//	}
//	var result = Record{
//		Source: "DatasetRowMapper",
//		Values: values,
//	}
//	return &result, nil
//
//}
//
//func newDatasetRowMapper(columns []string, columnToIndexMap map[string]int) dsc.RecordMapper {
//	if columnToIndexMap == nil {
//		index := 0
//		columnToIndexMap = make(map[string]int)
//		toolbox.SliceToMap(columns, columnToIndexMap, toolbox.CopyStringValueProvider, func(column string) int {
//			index++
//			return index
//		})
//	}
//
//	var result dsc.RecordMapper = &datasetRowMapper{
//		columns:          columns,
//		columnToIndexMap: columnToIndexMap,
//	}
//	return result
//}
