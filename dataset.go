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
	"github.com/viant/assertly"
	"strings"
	"sort"
)

const (
	AutoincrementDirective = "@autoincrement@"
	FromQueryDirective     = "@fromQuery@"
)

//Records represent data records
type Dataset struct {
	Table   string  `required:"true"`
	Records Records `required:"true"`
}

//NewDataset creates a new dataset for supplied table and records.
func NewDataset(table string, records ... map[string]interface{}) *Dataset {
	return &Dataset{
		Table:   table,
		Records: records,
	}
}

//Records represents table records
type Records []map[string]interface{}

//Records returns non empty records //directive a filtered out
func (r *Records) Expand(context toolbox.Context, includeDirectives bool) (result []interface{}, err error) {
	result = make([]interface{}, 0)
	var evaluator = assertly.NewDefaultMacroEvaluator()
	for _, candidate := range (*r) {
		record := Record(candidate)
		recordValues := make(map[string]interface{})
		var keys = record.Columns()
		if includeDirectives {
			keys = toolbox.MapKeysToStringSlice(record)
		}
		for _, k := range keys {
			v := record[k]
			recordValues[k] = v
			if text, ok := v.(string); ok {
				if recordValues[k], err = evaluator.Expand(context, text); err != nil {
					return nil, err
				}
			}
		}

		if len(recordValues) > 0 {
			result = append(result, recordValues)
		}
	}
	return result, nil
}

//ShouldDeleteAll checks if dataset contains empty record (indicator to delete all)
func (r *Records) ShouldDeleteAll() bool {
	var result = len(*r) == 0
	directiveScan(*r, func(record Record) {
		if record == nil || len(record) == 0 {
			result = true
		}
	})
	return result
}

//UniqueKeys returns value for unique key directive, it test keys in the following order: @Autoincrement@, @IndexBy@
func (r *Records) UniqueKeys() []string {
	var result []string
	directiveScan(*r, func(record Record) {
		for k, v := range record {
			if k == AutoincrementDirective || k == assertly.IndexByDirective {
				if keys, ok := v.([]string); ok {
					result = keys
				} else {
					result = strings.Split(toolbox.AsString(v), ",")
				}
			}
		}
	})
	return result
}

//FromQuery returns value for @FromQuery@ directive
func (r *Records) FromQuery() string {
	var result string
	directiveScan(*r, func(record Record) {
		for k, v := range record {
			if k == FromQueryDirective {
				result = toolbox.AsString(v)
			}
		}
	})
	return result
}

//PrimaryKey returns primary key directive if matched in the following order: @Autoincrement@, @IndexBy@
func (r *Records) Autoincrement() bool {
	var result = false
	directiveScan(*r, func(record Record) {
		for k := range record {
			if k == AutoincrementDirective {
				result = true
			}
		}
	})
	return result
}

//Columns returns unique column names for this dataset
func (r *Records) Columns() []string {
	var result = make([]string, 0)
	var unique = make(map[string]bool)
	for _, record := range *r {
		var actualRecord = Record(record)
		for _, column := range actualRecord.Columns() {
			if _, has := unique[column]; has {
				continue
			}
			unique[column] = true
			result = append(result, column)
		}
	}
	sort.Strings(result)
	return result
}

//DatastoreDatasets represents a collection of datastore datasets
type DatastoreDatasets struct {
	Datastore string `required:"true" description:"register datastore"`
	Datasets  []*Dataset `description:"collection of dataset per table"`
	Data map[string][]map[string]interface{} `description:"map, where each pair represent table name and its records (backward compatibility)"`
}



//DatasetResource represents a dataset resource
type DatasetResource struct {
	*url.Resource      ` description:"data file location, csv, json, ndjson formats are supported"`
	*DatastoreDatasets `required:"true" description:"datastore datasets"`
	Prefix  string     ` description:"location data file prefix"`  //apply prefix
	Postfix string     ` description:"location data file postgix"` //apply suffix
}


func (r *DatasetResource) loadDataset() (err error) {
	if r.Resource.URL == "" {
		return errors.New("resource was empty")
	}

	r.Resource.Init()
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
	return err
}

//Loads dataset from specified resource or data map
func (r *DatasetResource) Load() (err error) {
	if len(r.Datasets) == 0 {
		r.Datasets = make([]*Dataset, 0)
	}
	if r.Resource != nil {
		if err = r.loadDataset();err != nil {
			return err
		}
	}
	if len(r.Data) > 0 {
		for k, v := range r.Data {
			r.Datasets = append(r.Datasets, NewDataset(k, v...))
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
			r.Datasets = append(r.Datasets, dataSet)
			return nil
		}

	}
	err := json.NewDecoder(bytes.NewReader(data)).Decode(&dataSet.Records);
	if err != nil {
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

func NewDatasetResource(datastore string, URL, prefix, postfix string, datasets ...*Dataset) *DatasetResource {
	var result =  &DatasetResource{
		Resource: url.NewResource(URL),
		DatastoreDatasets: &DatastoreDatasets{
			Datastore: datastore,
			Datasets:datasets,
		},
		Prefix:  prefix,
		Postfix: postfix,
	}
	return result
}
