package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/assertly"
	"github.com/viant/toolbox/url"
	"fmt"
	"github.com/viant/toolbox"
)

//backward compatible struct and abstraction

//InitDatastoreRequest represent init datastore request
type V1InitDatastoreRequest struct {
	DatastoreConfigs []*V1DatastoreConfig
}

//DatastoreConfig represents datastore config
type V1DatastoreConfig struct {
	Datastore      string      //name of datastore registered in manager registry
	Config         *dsc.Config // datastore manager config
	ConfigURL      string      //url with Config JSON.
	AdminDbName    string      //optional admin datastore name, needed for sql datastore to drop/create database
	ClearDatastore bool        //flag to reset datastore (depending on dialablable it could be either drop/create datastore for CanDrop/CanCreate dialects, or drop/create tables
	Descriptors    []*dsc.TableDescriptor
	DatasetMapping map[string]*Mapping //key represent name of dataset to be mapped
}

//ExecuteScriptRequest represent datastore script request.
type V1ExecuteScriptRequest struct {
	Scripts []*V1Script
}

//Script represents a datastore  script
type V1Script struct {
	Datastore string
	Url       string
	Sqls      []string
	Body      string
}

type V1PrepareDatastoreRequest struct {
	Prepare []*V1Datasets
}

//ExpectDatasetRequest represent datastore verification request.
type V1ExpectDatasetRequest struct {
	Expect      []*V1Datasets
	CheckPolicy int
}

//Datasets represents a collection of Dataset's for Datastore
type V1Datasets struct {
	Datastore string
	Datasets  []*V1Dataset
}

func (d *V1Datasets) AsDatasets() []*Dataset {
	var result = make([]*Dataset, 0)
	for _, dataset := range d.Datasets {
		result = append(result, dataset.AsDataset())
	}
	return result
}

//V1Dataset
type V1Dataset struct {
	*dsc.TableDescriptor
	Rows []*V1Row
}

func (d *V1Dataset) AsDataset() *Dataset {
	var result = &Dataset{
		Table:   d.Table,
		Records: make([]map[string]interface{}, 0),
	}

	var directiveMap = make(map[string]interface{})
	if d.FromQuery != "" {
		directiveMap[FromQueryDirective] = d.FromQuery
	}
	if len(d.PkColumns) > 0 {
		directiveMap[assertly.IndexByDirective] = d.PkColumns
		if d.Autoincrement {
			directiveMap[AutoincrementDirective] = d.PkColumns[0]
		}
	}
	if len(directiveMap) > 0 {
		result.Records = append(result.Records, directiveMap)
	}
	for _, row := range d.Rows {
		result.Records = append(result.Records, row.Values)
	}
	return result
}

//Row represents dataset row
type V1Row struct {
	Values map[string]interface{}
	Source string
}

//V1AssertViolation represent AssertViolation
type V1AssertViolation struct {
	Datastore string
	Type      string
	Table     string
	Key       string
	Expected  string
	Actual    string
	Source    string
	Path      string
}

//V1ExpectDatasetResponse represents a test violations.
type V1ExpectDatasetResponse struct {
	*BaseResponse
	Violations    []*V1AssertViolation
	HasViolations bool
}

type V1Service struct {
	service Service
}

//Init creates datastore manager and register it in manaer registry, if ClearDatastore flag is set it will drop and create datastore.
func (s *V1Service) Init(request *V1InitDatastoreRequest) *BaseResponse {
	var response = &BaseResponse{Status: "ok"}
	for _, register := range request.DatastoreConfigs {
		if register.ConfigURL != "" && register.Config == nil {
			register.ConfigURL = ExpandTestProtocolAsPathIfNeeded(register.ConfigURL)
			resource := url.NewResource(register.ConfigURL)
			if err := resource.JSONDecode(&register); err != nil {
				response.SetErrror(err)
				return response
			}
		}
		registerResponse := s.service.Register(&RegisterRequest{
			Datastore:         register.Datastore,
			Config:            register.Config,
			AdminDatastore:    register.AdminDbName,
			RecreateDatastore: register.ClearDatastore,
			Descriptors:       register.Descriptors,
		})

		if registerResponse.Status != "ok" {
			response = registerResponse.BaseResponse
			return response
		}

		response.Message += fmt.Sprintf("registered %v (%v)\n", register.Datastore, register.Config.DriverName)
		if len(register.DatasetMapping) > 0 {
			mappingRequest := &MappingRequest{Mappings: make([]*Mapping, 0)}
			for k, mapping := range register.DatasetMapping {
				if mapping.Name == "" {
					mapping.Name = k
				}
				mappingRequest.Mappings = append(mappingRequest.Mappings, mapping)
				response.Message += fmt.Sprintf("mapping %v\n", mapping.Name)
			}
			mappingResponse := s.service.AddTableMapping(mappingRequest)
			if mappingResponse.Status != "ok" {
				response = mappingResponse.BaseResponse
				return response
			}
		}
	}
	return response
}

//ExecuteScripts executes script defined in the request
func (s *V1Service) ExecuteScripts(request *V1ExecuteScriptRequest) *BaseResponse {
	var response = &BaseResponse{Status: "ok"}
	if len(request.Scripts) == 0 {
		return response
	}
	var datastore string
	var scripts = make([]*url.Resource, 0)
	var SQLs = make([]string, 0)
	for _, script := range request.Scripts {
		if script.Url != "" {
			scripts = append(scripts, url.NewResource(ExpandTestProtocolAsPathIfNeeded(script.Url)))
		}
		if len(script.Sqls) > 0 {
			SQLs = append(SQLs, script.Sqls...)
		}
		datastore = script.Datastore
	}

	if len(scripts) > 0 {
		scriptResponse := s.service.RunScript(&RunScriptRequest{
			Datastore: datastore,
			Scripts:   scripts,
		})
		response = scriptResponse.BaseResponse
		if response.Status != "ok" {
			return response
		}

	}
	if len(SQLs) > 0 {
		scriptResponse := s.service.RunSQLs(&RunSQLRequest{
			Datastore: datastore,
			SQLs:      SQLs,
		})
		response = scriptResponse.BaseResponse
		if response.Status != "ok" {
			return response
		}
	}
	return response
}

//PrepareDatastore prepare datastore
func (s *V1Service) PrepareDatastore(request *V1PrepareDatastoreRequest) *BaseResponse {
	var response = &BaseResponse{Status: "ok"}
	for _, datasets := range request.Prepare {
		prepareRequest := &PrepareRequest{
			DatasetResource: &DatasetResource{
				DatastoreDatasets: &DatastoreDatasets{
					Datastore: datasets.Datastore,
					Datasets:  datasets.AsDatasets(),
				},
			},
		}
		prepareResponse := s.service.Prepare(prepareRequest)
		response = prepareResponse.BaseResponse
		if prepareResponse.Status != "ok" {
			return response
		}
		for _, modification := range prepareResponse.Modification {
			response.Message = fmt.Sprintf("\t%v: added:%d, modified:%d, deleted:%d\n", modification.Subject, modification.Added, modification.Modified, modification.Deleted)
		}
	}
	return response
}

//ExpectDatasets verifies that passed in expected dataset data values are present in the datastore, this methods reports any violations.
func (s *V1Service) ExpectDatasets(checkPolicy int, request *V1ExpectDatasetRequest) *V1ExpectDatasetResponse {
	response := &V1ExpectDatasetResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
		Violations:   make([]*V1AssertViolation, 0),
	}
	for _, datasets := range request.Expect {
		expectRequest := &ExpectRequest{
			DatasetResource: &DatasetResource{
				DatastoreDatasets: &DatastoreDatasets{
					Datastore: datasets.Datastore,
					Datasets:  datasets.AsDatasets(),
				},
			},
		}
		expectResponse := s.service.Expect(expectRequest)
		response.BaseResponse = expectResponse.BaseResponse
		if response.Status != "ok" {
			return response
		}

		for _, validation := range expectResponse.Validation {
			response.Message = fmt.Sprintf("%v %v: passed:%v, failed:%v\n", datasets.Datastore, validation.Dataset, validation.PassedCount, validation.FailedCount)
			if validation.FailedCount > 0 {
				for _, failure := range validation.Failures {
					response.Violations = append(response.Violations, &V1AssertViolation{
						Datastore: datasets.Datastore,
						Type:      failure.Reason,
						Table:     validation.Dataset,
						//Key   ://TODO Add key directive or reuse source directive
						Expected: toolbox.AsString(failure.Expected),
						Actual:   toolbox.AsString(failure.Actual),
						Path:     failure.Path,
					})
				}
			}
		}
	}
	return response
}
