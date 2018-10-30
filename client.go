package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

type serviceClient struct {
	serverURL string
}

//Registry returns registry of registered database managers
func (c *serviceClient) Registry() dsc.ManagerRegistry {
	return dsc.NewManagerRegistry()
}

//Register register database connection
func (c *serviceClient) Register(request *RegisterRequest) *RegisterResponse {
	var response = &RegisterResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+registerURI, request, response)
	response.SetError(err)
	return response

}

//Recreate remove and creates datastore
func (c *serviceClient) Recreate(request *RecreateRequest) *RecreateResponse {
	var response = &RecreateResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+recreateURI, request, response)
	response.SetError(err)
	return response

}

//RunSQL runs supplied SQL
func (c *serviceClient) RunSQL(request *RunSQLRequest) *RunSQLResponse {
	var response = &RunSQLResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+sqlURI, request, response)
	response.SetError(err)
	return response

}

//RunScript runs supplied SQL scripts
func (c *serviceClient) RunScript(request *RunScriptRequest) *RunSQLResponse {
	var response = &RunSQLResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+scriptURI, request, response)
	response.SetError(err)
	return response

}

//Add table mapping
func (c *serviceClient) AddTableMapping(request *MappingRequest) *MappingResponse {
	var response = &MappingResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+mappingURI, request, response)
	response.SetError(err)
	return response

}

func (c *serviceClient) Init(request *InitRequest) *InitResponse {
	var response = &InitResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+initURI, request, response)
	response.SetError(err)
	return response
}

//Populate database with datasets
func (c *serviceClient) Prepare(request *PrepareRequest) *PrepareResponse {
	var response = &PrepareResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+prepareURI, request, response)
	response.SetError(err)
	return response

}

//Verify datastore with supplied expected datasets
func (c *serviceClient) Expect(request *ExpectRequest) *ExpectResponse {
	var response = &ExpectResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+expectURI, request, response)
	response.SetError(err)
	return response
}

//Query returns query from database
func (c *serviceClient) Query(request *QueryRequest) *QueryResponse {
	var response = &QueryResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+queryURI, request, response)
	response.SetError(err)
	return response

}

//Query returns query from database
func (c *serviceClient) Freeze(request *FreezeRequest) *FreezeResponse {
	var response = &FreezeResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+freezeURI, request, response)
	response.SetError(err)
	return response

}

//Sequence returns sequence for supplied tables
func (c *serviceClient) Sequence(request *SequenceRequest) *SequenceResponse {
	var response = &SequenceResponse{BaseResponse: NewBaseOkResponse()}
	err := toolbox.RouteToService("post", c.serverURL+sequenceURI, request, response)
	response.SetError(err)
	return response
}

func (s *serviceClient) SetContext(context toolbox.Context) {

}

//NewServiceClient returns a new dsunit service client
func NewServiceClient(serverURL string) Service {
	var result Service = &serviceClient{serverURL: serverURL}
	return result
}
