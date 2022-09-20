package dsunit

import (
	"fmt"
	"github.com/viant/toolbox"
	"path"
	"testing"
)

var LogF = fmt.Printf

type Tester interface {

	// Register registers new datastore connection
	Register(t *testing.T, request *RegisterRequest) bool

	// RegisterFromURL registers new datastore connection, JSON request is fetched from URL
	RegisterFromURL(t *testing.T, URL string) bool

	// Recreate recreates datastore
	Recreate(t *testing.T, request *RecreateRequest) bool

	// RecreateFromURL recreates datastore, JSON request is fetched from URL
	RecreateFromURL(t *testing.T, URL string) bool

	// RunSQL runs supplied SQL
	RunSQL(t *testing.T, request *RunSQLRequest) bool

	// RunSQLFromURL runs supplied SQL, JSON request is fetched from URL
	RunSQLFromURL(t *testing.T, URL string) bool

	// RunScript runs supplied SQL scripts
	RunScript(t *testing.T, request *RunScriptRequest) bool

	// RunScriptFromURL runs supplied SQL scripts, JSON request is fetched from URL
	RunScriptFromURL(t *testing.T, URL string) bool

	// AddTableMapping adds table mapping
	AddTableMapping(t *testing.T, request *MappingRequest) bool

	// AddTableMappingFromURL adds table mapping, JSON request is fetched from URL
	AddTableMappingFromURL(t *testing.T, URL string) bool

	//Init inits datastore, (register, recreated, run sql, add mapping)
	Init(t *testing.T, request *InitRequest) bool

	// InitFromURL inits datastore, (register, recreated, run sql, add mapping), JSON request is fetched from URL
	InitFromURL(t *testing.T, URL string) bool

	// Prepare populates database with datasets
	Prepare(t *testing.T, request *PrepareRequest) bool

	// PrepareFromURL populates database with datasets, JSON request is fetched from URL
	PrepareFromURL(t *testing.T, URL string) bool

	// PopulateWithURL populates database with datasets, JSON requests are fetched from files in directory
	PopulateWithURL(t *testing.T, URL string, datastore string, datasets ...*Dataset) bool

	//PrepareDatastore matches all dataset files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name.
	PrepareDatastore(t *testing.T, datastore string) bool

	// PrepareFor matches all dataset files that are located in baseDirectory with method name and
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
	PrepareFor(t *testing.T, datastore string, baseDirectory string, method string) bool

	// Expect verifies datastore with supplied expected datasets
	Expect(t *testing.T, request *ExpectRequest) bool

	// ExpectFromURL verifies datastore with supplied expected datasets, JSON request is fetched from URL
	ExpectFromURL(t *testing.T, URL string) bool

	// ExpectWithURL verifies datastore with supplied expected datasets, JSON requests are fetched from files in directory
	ExpectWithURL(t *testing.T, checkPolicy int, URL string, datastore string, datasets ...*Dataset) bool

	// ExpectDatasets matches all dataset files that are located in the same directory as the test file with method name to
	// verify that all listed dataset values are present in datastore
	ExpectDatasets(t *testing.T, datastore string, checkPolicy int) bool

	// ExpectFor matches all dataset files that are located in baseDirectory with method name to
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
	ExpectFor(t *testing.T, datastore string, checkPolicy int, baseDirectory string, method string) bool

	// Ping waits until database is online or error
	Ping(t *testing.T, datastore string, timeoutMs int) bool
}

type localTester struct {
	service Service
}

func handleError(t *testing.T, err error) {
	if err != nil {
		file, method, line := toolbox.DiscoverCaller(2, 10, "stack_helper.go", "static.go", "tester.go", "helper.go")
		_, file = path.Split(file)
		fmt.Printf("%v:%v (%v)\n%v\n", file, line, method, err)
		t.FailNow()
	}
}

func handleResponse(t *testing.T, response *BaseResponse) bool {
	file, method, line := toolbox.DiscoverCaller(3, 10, "stack_helper.go", "static.go", "tester.go", "helper.go")
	_, file = path.Split(file)
	if response.Status != StatusOk {
		_, _ = LogF("%v:%v (%v)\n%v\n", file, line, method, response.Message)
		t.Fail()
		return false
	}
	return true
}

// Register registers new datastore connection
func (s *localTester) Register(t *testing.T, request *RegisterRequest) bool {
	response := s.service.Register(request)
	return handleResponse(t, response.BaseResponse)
}

// RegisterFromURL registers new datastore connection, JSON request is fetched from URL
func (s *localTester) RegisterFromURL(t *testing.T, URL string) bool {
	request, err := NewRegisterRequestFromURL(URL)
	handleError(t, err)
	return s.Register(t, request)
}

// Recreate recreates datastore
func (s *localTester) Recreate(t *testing.T, request *RecreateRequest) bool {
	response := s.service.Recreate(request)
	return handleResponse(t, response.BaseResponse)
}

// RecreateFromURL Recreate recreates datastore, JSON request is fetched from URL
func (s *localTester) RecreateFromURL(t *testing.T, URL string) bool {
	request, err := NewRecreateRequestFromURL(URL)
	handleError(t, err)
	return s.Recreate(t, request)
}

// RunSQL runs supplied SQL
func (s *localTester) RunSQL(t *testing.T, request *RunSQLRequest) bool {
	response := s.service.RunSQL(request)
	return handleResponse(t, response.BaseResponse)
}

// RunSQLFromURL runs supplied SQL, JSON request is fetched from URL
func (s *localTester) RunSQLFromURL(t *testing.T, URL string) bool {
	request, err := NewRunSQLRequestFromURL(URL)
	handleError(t, err)
	return s.RunSQL(t, request)
}

// RunScript runs supplied SQL scripts
func (s *localTester) RunScript(t *testing.T, request *RunScriptRequest) bool {
	response := s.service.RunScript(request)
	return handleResponse(t, response.BaseResponse)
}

// RunScriptFromURL runs supplied SQL scripts, JSON request is fetched from URL
func (s *localTester) RunScriptFromURL(t *testing.T, URL string) bool {
	request, err := NewRunScriptRequestFromURL(URL)
	handleError(t, err)
	return s.RunScript(t, request)
}

// AddTableMapping adds table mapping
func (s *localTester) AddTableMapping(t *testing.T, request *MappingRequest) bool {
	response := s.service.AddTableMapping(request)
	return handleResponse(t, response.BaseResponse)
}

// AddTableMappingFromURL adds table mapping, JSON request is fetched from URL
func (s *localTester) AddTableMappingFromURL(t *testing.T, URL string) bool {
	request, err := NewMappingRequestFromURL(URL)
	handleError(t, err)
	return s.AddTableMapping(t, request)
}

// Init inits datastore, (register, recreated, run sql, add mapping)
func (s *localTester) Init(t *testing.T, request *InitRequest) bool {
	response := s.service.Init(request)
	return handleResponse(t, response.BaseResponse)

}

// InitFromURL inits datastore, (register, recreated, run sql, add mapping), JSON request is fetched from URL
func (s *localTester) InitFromURL(t *testing.T, URL string) bool {
	request, err := NewInitRequestFromURL(URL)
	handleError(t, err)
	return s.Init(t, request)
}

// Prepare populates database with datasets
func (s *localTester) Prepare(t *testing.T, request *PrepareRequest) bool {
	response := s.service.Prepare(request)
	return handleResponse(t, response.BaseResponse)
}

// PrepareFromURL populates database with datasets, JSON request is fetched from URL
func (s *localTester) PrepareFromURL(t *testing.T, URL string) bool {
	request, err := NewPrepareRequestFromURL(URL)
	handleError(t, err)
	return s.Prepare(t, request)
}

// PopulateWithURL populates database with datasets, JSON requests are fetched from files in directory
func (s *localTester) PopulateWithURL(t *testing.T, URL string, datastore string, datasets ...*Dataset) bool {
	populateSet := NewDatasetResource(datastore, URL, "", "", datasets...)
	return s.Prepare(t, NewPrepareRequest(populateSet))
}

// PrepareDatastore matches all dataset files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name.
func (s *localTester) PrepareDatastore(t *testing.T, datastore string) bool {
	URL, prefix := discoverBaseURLAndPrefix("prepare")
	request := &PrepareRequest{
		DatasetResource: NewDatasetResource(datastore, URL, prefix, ""),
	}
	return s.Prepare(t, request)
}

// PrepareFor matches all dataset files that are located in baseDirectory with method name and
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
func (s *localTester) PrepareFor(t *testing.T, datastore, baseDirectory, method string) bool {
	method = convertToLowerUnderscore(method)
	request := &PrepareRequest{
		DatasetResource: NewDatasetResource(datastore, baseDirectory, fmt.Sprintf("%v_prepare_", method), ""),
		Expand:          true,
	}
	return s.Prepare(t, request)
}

// Expect verifies datastore with supplied expected datasets
func (s *localTester) Expect(t *testing.T, request *ExpectRequest) bool {
	response := s.service.Expect(request)
	var result = handleResponse(t, response.BaseResponse)
	return result
}

// ExpectFromURL verifies datastore with supplied expected datasets, JSON request is fetched from URL
func (s *localTester) ExpectFromURL(t *testing.T, URL string) bool {
	request, err := NewExpectRequestFromURL(URL)
	handleError(t, err)
	return s.Expect(t, request)
}

// ExpectWithURL verifies datastore with supplied expected datasets, JSON requests are fetched from files in directory
func (s *localTester) ExpectWithURL(t *testing.T, checkPolicy int, URL string, datastore string, datasets ...*Dataset) bool {
	expextSet := NewDatasetResource(datastore, URL, "", "", datasets...)
	return s.Expect(t, NewExpectRequest(checkPolicy, expextSet))
}

// ExpectDatasets matches all dataset files that are located in the same directory as the test file with method name to
// verify that all listed dataset values are present in datastore
func (s *localTester) ExpectDatasets(t *testing.T, datastore string, checkPolicy int) bool {
	URL, prefix := discoverBaseURLAndPrefix("expect")
	request := &ExpectRequest{
		CheckPolicy:     checkPolicy,
		DatasetResource: NewDatasetResource(datastore, URL, prefix, ""),
	}
	return s.Expect(t, request)
}

// ExpectFor matches all dataset files that are located in baseDirectory with method name to
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
func (s *localTester) ExpectFor(t *testing.T, datastore string, checkPolicy int, baseDirectory, method string) bool {
	method = convertToLowerUnderscore(method)
	request := &ExpectRequest{
		DatasetResource: NewDatasetResource(datastore, baseDirectory, fmt.Sprintf("%v_expect_", method), ""),
	}
	return s.Expect(t, request)
}

func (s *localTester) Ping(t *testing.T, datastore string, timeoutMs int) bool {
	request := &PingRequest{Datastore: datastore, TimeoutMs: timeoutMs}
	response := s.service.Ping(request)
	return handleResponse(t, response.BaseResponse)
}

// NewTester creates a new local tester
func NewTester() Tester {
	return &localTester{service: New()}
}

// NewRemoveTester creates a new remove tester
func NewRemoveTester(endpoint string) Tester {
	return &localTester{service: NewServiceClient(endpoint)}
}
