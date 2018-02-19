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
	r.Status = "error"
	r.Message = err.Error()
}

//RegisterRequest represent register request

type RegisterRequest struct {
	Datastore         string      `required:"true" description:"datastore name"`
	Config            *dsc.Config `required:"true" description:"datastore config"`
	adminConfig       *dsc.Config
	AdminDatastore    string      `description:"admin datastore, needed to connect and create test database"`
	RecreateDatastore bool        `description:"flag to re create datastore"`
}

type RegisterResponse struct {
	*BaseResponse
}

type RunSQLRequest struct {
	Datastore string `required:"true" description:"registered datastore name"`
	SQLs      []string
}

type RunSQLResponse struct {
	*BaseResponse
	RowsAffected int
}

type RunScriptRequest struct {
	Datastore string `required:"true" description:"registered datastore name"`
	Scripts   []*url.Resource
}


type MappingRequest struct {
	Mappings []*Mapping `required:"true" description:"virtual table mapping"`
}

type MappingResponse struct {
	*BaseResponse
	Tables []string
}

type PrepareRequest struct {
	*DatasetResource `required:"true" description:"datasets resource"`
}

type PrepareResponse struct {
}

type ExpectRequest struct {
	*DatasetResource
	CheckPolicy int `required:"true" description:"0 - FullTableDatasetCheckPolicy, 1 - SnapshotDatasetCheckPolicy"`
}


type DatasetValidation struct {
	Dataset string
	*assertly.Validation
}


type ExpectResponse struct {
	Validation []*DatasetValidation
	Passed int
	Failed int
}


type NamedQuery struct {
	Name string  `required:"true"`
	SQL  string  `required:"true"`
}

type QueryExpectRequest struct {
	*DatasetResource `required:"true"`
	Actual *[]NamedQuery `required:"true"`
}

