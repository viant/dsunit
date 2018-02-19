package dsunit

import (
	"strings"
	"testing"
	"github.com/viant/toolbox"
)


var dsUnitService Service
var baseDirectory string

//SetService sets global dsunit service.
func SetService(service Service) {
	dsUnitService = service
}

//GetService returns dsunit service.
func GetService() Service {
	if dsUnitService == nil {
		baseDirectory = toolbox.CallerDirectory(5)
		dsUnitService = New(baseDirectory)
	}
	return dsUnitService
}






//InitDatastoreFromURL initialises datastore from URL, URL needs to point to  InitDatastoreRequest JSON
//it register datastore, table descriptor, data mapping, and optionally recreate datastore
func InitDatastoreFromURL(t *testing.T, url string) {
	service := GetService()
	response := service.InitFromURL(url)
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
func ExpectDatasetFor(t *testing.T, datastore string, checkPolicy int, baseDirectory string, method string) {
	service := GetService()
	response := service.ExpectDatasetsFor(datastore, baseDirectory, method, checkPolicy)
	handleResponse(t, response.Response)
}





//ExecuteScriptFromURL executes script from URL, URL should point to  ExecuteScriptRequest JSON.
func ExecuteScriptFromURL(t *testing.T, url string) {
	service := GetService()
	response := service.ExecuteScriptsFromURL(url)
	handleResponse(t, response)
}






//ExpandTestProtocolAsUrlIfNeeded extends input if it start with test:// fragment to currently test file directory as file protocol
func ExpandTestProtocolAsURLIfNeeded(input string) string {
	GetService()
	if strings.HasPrefix(input, TestSchema) {
		return toolbox.FileSchema + baseDirectory + input[len(TestSchema):]
	}
	return input
}


//ExpandTestProtocolAsPathIfNeeded extends input if it start with test:// fragment to currently test file directory
func ExpandTestProtocolAsPathIfNeeded(input string) string {
	GetService()
	if strings.HasPrefix(input, TestSchema) {
		return baseDirectory + input[len(TestSchema):]
	}
	return input
}
