package dsunit

import (
	"testing"
)

var tester = NewTester()

//Register registers new datastore connection
func Register(t *testing.T, request *RegisterRequest) bool {
	return tester.Register(t, request)
}

//Register registers new datastore connection, JSON request is fetched from URL
func RegisterFromURL(t *testing.T, URL string) bool {
	return tester.RegisterFromURL(t, URL)
}

//Recreate recreates datastore
func Recreate(t *testing.T, request *RecreateRequest) bool {
	return tester.Recreate(t, request)
}

//Recreate recreates datastore, JSON request is fetched from URL
func RecreateFromURL(t *testing.T, URL string) bool {
	return tester.RecreateFromURL(t, URL)
}

//RunSQL runs supplied SQL
func RunSQL(t *testing.T, request *RunSQLRequest) bool {
	return tester.RunSQL(t, request)
}

//RunSQL runs supplied SQL, JSON request is fetched from URL
func RunSQLFromURL(t *testing.T, URL string) bool {
	return tester.RunSQLFromURL(t, URL)
}

//RunScript runs supplied SQL scripts
func RunScript(t *testing.T, request *RunScriptRequest) bool {
	return tester.RunScript(t, request)
}

//RunScript runs supplied SQL scripts, JSON request is fetched from URL
func RunScriptFromURL(t *testing.T, URL string) bool {
	return tester.RunScriptFromURL(t, URL)
}

//Add table mapping
func AddTableMapping(t *testing.T, request *MappingRequest) bool {
	return tester.AddTableMapping(t, request)
}

//Add table mapping, JSON request is fetched from URL
func AddTableMappingFromURL(t *testing.T, URL string) bool {
	return tester.AddTableMappingFromURL(t, URL)
}

//Init datastore, (register, recreated, run sql, add mapping)
func Init(t *testing.T, request *InitRequest) bool {
	return tester.Init(t, request)
}

//Init datastore, (register, recreated, run sql, add mapping), JSON request is fetched from URL
func InitFromURL(t *testing.T, URL string) bool {
	return tester.InitFromURL(t, URL)
}

//Populate database with datasets
func Prepare(t *testing.T, request *PrepareRequest) bool {
	return tester.Prepare(t, request)
}

//Populate database with datasets, JSON request is fetched from URL
func PrepareFromURL(t *testing.T, URL string) bool {
	return tester.PrepareFromURL(t, URL)
}

//PrepareDatastore matches all dataset files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name.
func PrepareDatastore(t *testing.T, datastore string) bool {
	return tester.PrepareDatastore(t, datastore)

}

//PrepareFor matches all dataset files that are located in baseDirectory with method name and
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
func PrepareFor(t *testing.T, datastore string, baseDirectory string, method string) bool {
	return tester.PrepareFor(t, datastore, baseDirectory, method)
}

//Verify datastore with supplied expected datasets
func Expect(t *testing.T, request *ExpectRequest) bool {
	return tester.Expect(t, request)
}

//Verify datastore with supplied expected datasets, JSON request is fetched from URL
func ExpectFromURL(t *testing.T, URL string) bool {
	return tester.ExpectFromURL(t, URL)
}

//ExpectDatasets matches all dataset files that are located in the same directory as the test file with method name to
//verify that all listed dataset values are present in datastore
func ExpectDatasets(t *testing.T, datastore string, checkPolicy int) bool {
	return tester.ExpectDatasets(t, datastore, checkPolicy)
}

//ExpectFor matches all dataset files that are located in baseDirectory with method name to
// verify that all listed dataset values are present in datastore
// Note the matchable dataset files in the base directory have the following naming:
//
//  <lower_underscore method name>_expect_<table>.[json|csv]
//  To prepare expected dataset table: 'users' and 'permissions' for test method ReadAll you would
//  have you create the following files in the baseDirectory
//
//  read_all_expect_users.json
//  read_all_expect_permissions.json
//
func ExpectFor(t *testing.T, datastore string, checkPolicy int, baseDirectory string, method string) bool {
	return tester.ExpectFor(t, datastore, checkPolicy, baseDirectory, method)
}

//UseRemoteTestServer enables remove testing mode
func UseRemoteTestServer(endpoint string) {

}
