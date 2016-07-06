package dsunit

import (
	"github.com/viant/toolbox"
)

type serviceClient struct {
	serviceLocal
	serverURL string
}

func getResponseOrErrorResponse(response *Response, err error) *Response {
	if err != nil {
		return newErrorResponse(err)
	}
	return response
}

func (c *serviceClient) Init(request *InitDatastoreRequest) *Response {
	for i := range request.DatastoreConfigs {
		_, err := c.initDatastorFromConfig(&request.DatastoreConfigs[i])
		if err != nil {
			return newErrorResponse(err)
		}
	}
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+initURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) ExecuteScripts(request *ExecuteScriptRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+executeURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) PrepareDatastore(request *PrepareDatastoreRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+prepareURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

func (c *serviceClient) ExpectDatasets(request *ExpectDatasetRequest) *Response {
	response := &Response{}
	err := toolbox.RouteToService("post", c.serverURL+expectURI, request, response)
	return getResponseOrErrorResponse(response, err)
}

//NewServiceClient returns a new dsunit service client
func NewServiceClient(testDirectory, serverURL string) Service {
	datasetTestManager := NewDatasetTestManager()
	var localService = serviceLocal{testManager: datasetTestManager, testDirectory: testDirectory}
	var dsUnitClient = &serviceClient{serviceLocal: localService, serverURL: serverURL}
	var result Service = dsUnitClient
	localService.service = result
	dsUnitClient.service = result
	return result
}
