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
/*

Datastore testibility library.




Usage


    dsunit.InitDatastoresFromUrl(t, "test://test/datastore_init.json")
    dsunit.ExecuteScriptFromUrl(t, "test://test/script_request.json")
    dsunit.PrepareDatastore(t, "mytestdb")


    ... business test logic comes here

    dsunit.ExpectDatasets(t, "mytestdb", dsunit.SnapshotDatasetCheckPolicy)


 */
package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)


const (
	//FullTableDatasetCheckPolicy policy will drive comparison of all actual datastore data
	FullTableDatasetCheckPolicy = 0
	//SnapshotDatasetCheckPolicy policy will drive comparison of subset of  actual datastore data that is is listed in expected dataset
	SnapshotDatasetCheckPolicy = 1
)

//Row represents dataset row
type Row struct {
	Values map[string]interface{}
	Source string
}

//Dataset represents test or expected dataset data values
type Dataset struct {
	dsc.TableDescriptor
	Rows []Row
}

//Datasets represents a collection of Dataset's for Datastore
type Datasets struct {
	Datastore string
	Datasets  []Dataset
}

//Script represents a datastore  script
type Script struct {
	Datastore string
	URL       string
	SQLs      []string
	Body      string
}


//DatasetTestManager supervises datastore initialization, test dataset preparation, and final datastore dataset verification
type DatasetTestManager interface {

	//ClearDatastore clears datastore, it takes adminDatastore and targetDatastore names.
	ClearDatastore(adminDatastore string, targetDatastore string) (error)

	//Execute executes passed in script, script defines what database it run on.
	Execute(script *Script) (int, error)

	//ExecuteFromUrl reads content from url and executes it on datastore
	ExecuteFromURL(datastore string, url string) (int, error)

	//PrepareDatastore prepare datastore datasets by adding, updating or deleting data.
	// Rows will be added if they weren't present, updated if they were present, and deleted if passed in dataset has not rows defined.
	PrepareDatastore(datasets *Datasets) (inserted, updated, deleted int, err error)

	//ExpectDatasets verifies that passed in expected dataset data values are present in the datastore, this methods reports any violations.
	ExpectDatasets(checkPolicy int, expected *Datasets) (AssertViolations, error)

	//ManagerRegistry returns ManagerRegistry.
	ManagerRegistry()  dsc.ManagerRegistry

	//ValueProviderRegistry returns macro value provider registry.
	ValueProviderRegistry() toolbox.ValueProviderRegistry

	//MacroEvaluator returns macro evaluator.
	MacroEvaluator() *toolbox.MacroEvaluator

	//DatasetFactory returns dataset factory.
	DatasetFactory() DatasetFactory

	//RegisterTable register table descriptor within datastore manager.
	RegisterTable(datastore string, tableDescriptor *dsc.TableDescriptor)

	//RegisteredTables returns all registered table for passed in datastore.
	RegisteredTables(datastore string) []string

	//RegisterDatasetMapping registers dataset mapping for passed in name.
	//Note that dataset mapping name should never be the actual table name, as this method will create table descriptor for the mapping.
	RegisterDatasetMapping(name string, mapping *DatasetMapping)

	//RegisteredMapping returns all registered dataset mapping
	RegisteredMapping() []string

}

//DatasetFactory represents a dataset factory.
type DatasetFactory interface {

	//Create creates a dataset from map for passed in table descriptor
	Create(descriptor *dsc.TableDescriptor, dataset ...map[string]interface{}) *Dataset

	//CreateFromMap crate a dataset from a map for passed in datastore and table
	CreateFromMap(datastore string, table string, dataset ...map[string]interface{}) *Dataset

	//CreateFromMap crate a dataset from a map for passed in datastore and table
	CreateFromURL(datastore string, table string, url string) (*Dataset, error)


}


//DatasetColumn represents dataset mapping column.
type DatasetColumn struct {
	Name         string
	DefaultValue string
	FromColumn   string
	Required     bool
}


//DatasetMapping represents a dataset mapping, mapping allow to route data defined in only one dataset to many datasets.
type DatasetMapping struct {
	Table        string
	Columns      []DatasetColumn
	Associations []DatasetMapping
}



//AssertViolation represents test violation.
type AssertViolation struct {
	Datastore string
	Type      string
	Table     string
	Key       string
	Expected  string
	Actual    string
	Source    string
}


//AssertViolations represents a test violations.
type AssertViolations interface {

	Violations() []AssertViolation

	HasViolations() bool

	String() string
}


//DatastoreConfig represets DatastoreConfig dsunit config
type DatastoreConfig struct {
	Datastore      string                    //name of datastore registered in manager registry
	Config         dsc.Config                // datastore manager config
	ConfigURL      string                    //url with Config JSON.
	AdminDbName    string                    //optional admin datastore name, needed for sql datastore to drop/create database
	ClearDatastore bool                      //flag to reset datastore (depending on dialablable it could be either drop/create datastore for CanDrop/CanCreate dialects, or drop/create tables
	Descriptors    []dsc.TableDescriptor
	DatasetMapping map[string]DatasetMapping //key represent name of dataset to be mapped
}


//Service represents test service
type Service interface {

	//Init creates datastore manager and register it in manaer registry, if ClearDatastore flag is set it will drop and create datastore.
	Init(request *InitDatastoreRequest) *Response

	//InitFromUrl reads from url  InitDatastoresRequest JSON and initializes
	InitFromURL(url string) *Response

	//ExecuteScripts executes script defined in the request
	ExecuteScripts(request *ExecuteScriptRequest) *Response

	//ExecuteScripts loads ExecuteScriptsExecuteScripts JSON from url and executes it.
	ExecuteScriptsFromURL(url string) *Response

	//PrepareDatastore prepares datastore for request, see DatasetTestManager#PrepareDatastore
	PrepareDatastore(request *PrepareDatastoreRequest) *Response

	//PrepareDatastore laods PrepareDatastoreRequest JSON from url to prepare datastore, see DatasetTestManager#PrepareDatastore
	PrepareDatastoreFromURL(url string) *Response

	//PrepareDatastore prepares for passed in datastore, it loads matching dataset files from based directory and method.
	PrepareDatastoreFor(datastore string, baseDir string, method string) *Response

	//ExpectDatasets verifies dataset data in datastore for passed in request, see DatasetTestManager#ExpectDataset
	ExpectDatasets(request *ExpectDatasetRequest) *Response

	//ExpectDatasets loads ExpectDatasetRequest json from url to verify dataset, see DatasetTestManager#ExpectDataset
	ExpectDatasetsFromURL(url string) *Response

	//ExpectDatasetsFor verifies datastore for passed in datastore, it loads matching dataset files from based directory and method.
	ExpectDatasetsFor(datastore string, baseDir string, method string, checkPolicy int) *Response
}

//Response represetns a response.
type Response struct {
	Status  string
	Message string
}

//InitDatastoreRequest represent initalization in dsunit service datastore request.
type InitDatastoreRequest struct {
	DatastoreConfigs []DatastoreConfig
}

//ExecuteScriptRequest represent datastore script request.
type ExecuteScriptRequest struct {
	Scripts []Script
}

//PrepareDatastoreRequest represent datastore prepare request.
type PrepareDatastoreRequest struct {
	Datasets []Datasets
}

//ExpectDatasetRequest represent datastore verification request.
type ExpectDatasetRequest struct {
	Datasets    []Datasets
	CheckPolicy int
}



