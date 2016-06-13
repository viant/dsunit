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
	"strings"
	"runtime"
	"testing"
	"github.com/viant/toolbox"
	"path"
)


var dsUnitService Service;
var baseDirectory string;

//SetService sets global dsunit service.
func SetService(service Service) {
	dsUnitService = service
}

//GetService returns dsunit service.
func GetService() Service {
	if dsUnitService == nil {
		file, _, _ := getCallerInfo(4)
		baseDirectory = path.Dir(file) + "/"
		dsUnitService =  NewServiceLocal(baseDirectory)
	}
	return dsUnitService
}

//UseRemoteTestServer this method changes service to run all operation remotely using passed in URL.
func UseRemoteTestServer(serverURL string) {
	file, _, _ := getCallerInfo(3)
	baseDirectory := path.Dir(file)+ "/"
	SetService(NewServiceClient(baseDirectory, serverURL))
}


func getCallerFileAndMethod() (string, string) {
	file, method, _ := getCallerInfo(3)
	return file, method
}


func handleResponse(t *testing.T, response *Response) {
	if response.hasError() {
		file, method, line := getCallerInfo(4)
		_, file = path.Split(file)
		t.Errorf("\n%v.%v:%v %v",file, method, line, response.Message)
		t.FailNow()
	} else {
		t.Logf(response.Message)
	}
}


func getCallerInfo(callerIndex int) (string, string, int) {
	var callerPointer = make([]uintptr, 10)  // at least 1 entry needed
	runtime.Callers(callerIndex, callerPointer)
	callerInfo := runtime.FuncForPC(callerPointer[0])
	file, line := callerInfo.FileLine(callerPointer[0])
	callerName := callerInfo.Name()
	dotPosition := strings.LastIndex(callerName, ".")
	return file, callerName[dotPosition + 1:len(callerName)], line
}



//PrepareDatastore matches all dataset files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name.
func PrepareDatastore(t *testing.T, datastore string) {
	testFile, method, _ := getCallerInfo(3)
	pathPrefix := removeFileExtension(testFile)
	service := GetService()
	response := service.PrepareDatastoreFor(datastore, pathPrefix +"_", method)
	handleResponse(t, response)
}



//PrepareDatastoreFor matches all dataset files that are located in baseDirectory with method name and
// populate datastore with all listed dataset
// Note the matchable dataset files in the base directory have the following naming:
//
//  <lower_underscore method name>_populate_<table>.[json|csv]
//  To prepare dataset to populate datastore table: 'users' and 'permissions' for test method ReadAll you would
//  have you create the following files in the baseDirectory
//
//  read_all_prepare_travelers2.json
//  read_all_populate_permissions.json
//
func PrepareDatastoreFor(t *testing.T, datastore string, baseDirectory string, method string) {
	service := GetService()
	response := service.PrepareDatastoreFor(datastore, baseDirectory, method)
	handleResponse(t, response)
}

//ExpectDatasets matches all dataset files that are located in the same directory as the test file with method name and
//verifies that all listed dataset values are present in datastore
func ExpectDatasets(t *testing.T, datastore string, checkPolicy int) {
	file, method, _ := getCallerInfo(3)
	pathPrefix := removeFileExtension(file)
	service := GetService()
	response := service.ExpectDatasetsFor(datastore, pathPrefix +"_", method, checkPolicy)
	handleResponse(t, response)
}



//ExpectDatasetFor matches all dataset files that are located in baseDirectory with method name and
// verifies that all listed dataset values are present in datastore
// Note the matchable dataset files in the base directory have the following naming:
//
//  <lower_underscore method name>_expect_<table>.[json|csv]
//  To prepare expected dataset table: 'users' and 'permissions' for test method ReadAll you would
//  have you create the following files in the baseDirectory
//
//  read_all_expect_users.json
//  read_all_expect_permissions.json
//
func ExpectDatasetFor(t *testing.T,  datastore string, checkPolicy int, baseDirectory string, method string) {
	service := GetService()
	response := service.ExpectDatasetsFor(datastore, baseDirectory, method, checkPolicy)
	handleResponse(t, response)
}



//InitDatastoreFromURL initialises datastore from URL, URL needs to point to  InitDatastoreRequest JSON
//it register datastore, table descriptor, data mapping, and optionally recreate datastore
func InitDatastoreFromURL(t *testing.T, url string) {
	service := GetService()
	response := service.InitFromURL(url)
	handleResponse(t, response)
}

//ExecuteScriptFromURL executes script from URL, URL should point to  ExecuteScriptRequest JSON.
func ExecuteScriptFromURL(t *testing.T, url string) {
	service := GetService()
	response := service.ExecuteScriptsFromURL(url)
	handleResponse(t, response)
}

//ExpandTestProtocolIfNeeded extends input if it start with test:// fragment to currently test file directory
func ExpandTestProtocolIfNeeded(input string) string {
	GetService()
	if strings.HasPrefix(input, TestSchema) {
		return toolbox.FileSchema + baseDirectory + input[len(TestSchema):len(input)]
	}
	return input
}