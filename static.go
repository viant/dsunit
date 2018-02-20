package dsunit

import (
	"strings"
	"testing"
	"github.com/viant/toolbox"
	"path"
	"github.com/viant/t/url"
	"fmt"
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
		baseDirectory = toolbox.CallerDirectory(3)
		dsUnitService = New()
	}
	return dsUnitService
}

func handleResponse(t *testing.T, response *BaseResponse) {
	if response.Status != "" {
		file, method, line := getCallerInfo(4)
		_, file = path.Split(file)
		t.Errorf("\n%v.%v:%v %v", file, method, line, response.Message)
		t.FailNow()
	} else {
		t.Logf(response.Message)
	}
}

//InitDatastoreFromURL initialises datastore from URL, URL needs to point to  InitDatastoreRequest JSON
//it register datastore, table descriptor, data mapping, and optionally recreate datastore
func InitDatastoreFromURL(t *testing.T, URL string) {
	service := GetService()
	var resource = url.NewResource(URL)
	request := &V1InitDatastoreRequest{}
	err := resource.JSONDecode(request)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	v1Service := &V1Service{service: service}
	response := v1Service.Init(request)
	handleResponse(t, response)
}

//PrepareDatastore matches all dataset files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name.
func PrepareDatastore(t *testing.T, datastore string) {
	testFile, method, _ := getCallerInfo(3)
	testFile = string(testFile[:len(testFile)-3])
	parent, name := path.Split(testFile)
	PrepareDatastoreFor(t, datastore, parent, fmt.Sprintf("%v_%v", name, method))
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
	method = convertToLowerUnderscore(method)
	baseDirectory = ExpandTestProtocolAsPathIfNeeded(baseDirectory)
	request := &PrepareRequest{
		DatasetResource: NewDatasetResource(datastore, baseDirectory, fmt.Sprintf("%v_populate_", method), ""),
	}
	response := service.Prepare(request)
	handleResponse(t, response.BaseResponse)
}

//ExpectDatasets matches all dataset files that are located in the same directory as the test file with method name and
//verifies that all listed dataset values are present in datastore
func ExpectDatasets(t *testing.T, datastore string, checkPolicy int) {
	testFile, method, _ := getCallerInfo(3)
	testFile = string(testFile[:len(testFile)-3])
	parent, name := path.Split(testFile)
	ExpectDatasetFor(t, datastore, checkPolicy, parent, fmt.Sprintf("%v_%v", name, method))
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
	method = convertToLowerUnderscore(method)
	baseDirectory = ExpandTestProtocolAsPathIfNeeded(baseDirectory)
	request := &ExpectRequest{
		CheckPolicy:     checkPolicy,
		DatasetResource: NewDatasetResource(datastore, baseDirectory, fmt.Sprintf("%v_populate_", method), ""),
	}
	response := service.Expect(request)
	handleResponse(t, response.BaseResponse)
}

//ExecuteScriptFromURL executes script from URL, URL should point to  ExecuteScriptRequest JSON.
func ExecuteScriptFromURL(t *testing.T, URL string) {
	service := GetService()
	var resource = url.NewResource(URL)
	request := &V1ExecuteScriptRequest{}
	err := resource.JSONDecode(request)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	v1Service := &V1Service{service: service}
	response := v1Service.ExecuteScripts(request)
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
