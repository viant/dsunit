package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox/url"
	"github.com/viant/assertly"
)

//BaseResponse represent base response.
type BaseResponse struct {
	Status  string
	Message string
}

func (r *BaseResponse) SetErrror(err error) {
	if err == nil {
		return
	}
	r.Status = "error"
	r.Message = err.Error()
}

//RegisterRequest represent register request
type RegisterRequest struct {
	Datastore         string                 `required:"true" description:"datastore name"`
	Config            *dsc.Config            `required:"true" description:"datastore config"`
	AdminConfig       *dsc.Config
	AdminDatastore    string                 `description:"admin datastore, needed to connect and create test database"`
	RecreateDatastore bool                   `description:"flag to re create datastore"`
	Descriptors       []*dsc.TableDescriptor `description:"optional table descriptors"`
}

//RegisterResponse represents register response
type RegisterResponse struct {
	*BaseResponse
}

//RunSQLRequest represents run SQL request
type RunSQLRequest struct {
	Datastore string `required:"true" description:"registered datastore name"`
	SQLs      []string
}

//RunSQLRequest represents run SQL response
type RunSQLResponse struct {
	*BaseResponse
	RowsAffected int
}

//RunScriptRequest represents run SQL Script request
type RunScriptRequest struct {
	Datastore string `required:"true" description:"registered datastore name"`
	Scripts   []*url.Resource
}

//MappingRequest represnet a mapping request
type MappingRequest struct {
	Mappings []*Mapping `required:"true" description:"virtual table mapping"`
}

//MappingResponse represents mapping response
type MappingResponse struct {
	*BaseResponse
	Tables []string
}

//PrepareRequest represents a request to populate datastore with data resource
type PrepareRequest struct {
	*DatasetResource `required:"true" description:"datasets resource"`
}

//ModificationInfo represents a modification info
type ModificationInfo struct {
	Subject  string
	Method   string `description:"modification method determined by presence of primary key: load - insert, persist: insert or update"`
	Deleted  int
	Modified int
	Added    int
}

//PrepareResponse represents a prepare response
type PrepareResponse struct {
	*BaseResponse
	Modification map[string]*ModificationInfo `description:"modification info by subject"`
}

//ExpectRequest represents verification datastore request
type ExpectRequest struct {
	*DatasetResource
	CheckPolicy int `required:"true" description:"0 - FullTableDatasetCheckPolicy, 1 - SnapshotDatasetCheckPolicy"`
}

//ExpectRequest represents data validation
type DatasetValidation struct {
	Dataset string
	*assertly.Validation
}





//ExpectResponse represents verification response
type ExpectResponse struct {
	*BaseResponse
	Validation  []*DatasetValidation
	PassedCount int
	FailedCount int
}

//NamedQuery represents a named query
type NamedQuery struct {
	Name string `required:"true"`
	SQL  string `required:"true"`
}
