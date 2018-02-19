package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/dsunit/script"
	"github.com/viant/toolbox/storage"
	"io"
)

//Service represents test service
type Service interface {
	//Registry returns registry of registered database managers
	Registry() dsc.ManagerRegistry

	//Register registers new datastore connection
	Register(request *RegisterRequest) *RegisterResponse

	//RunSQLs runs supplied SQLs
	RunSQLs(request *RunSQLRequest) *RunSQLResponse

	//RunScript runs supplied SQL scripts
	RunScript(request *RunScriptRequest) *RunSQLResponse

	//Add table mapping
	AddTableMapping(request *MappingRequest) *MappingResponse

	//Populate database with datasets
	Prepare(request *PrepareRequest) *PrepareResponse

	//Verify datastore with supplied expected datasets
	Expect(request *ExpectRequest) *ExpectResponse

	//Verify actual query data with supplied expected datasets
	QueryExpect(request *QueryExpectRequest) *ExpectResponse
}

type service struct {
	registry dsc.ManagerRegistry
	mapper   *Mapper
}

func (s *service) Registry() dsc.ManagerRegistry {
	return s.registry
}


func (s *service) Register(request *RegisterRequest) *RegisterResponse {

		
	return nil

}



func (s *service) RunSQLs(request *RunSQLRequest) *RunSQLResponse {
	var response = &RunSQLResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	manager := s.registry.Get(request.Datastore)
	results, err := manager.ExecuteAll(request.SQLs)
	if err != nil {
		response.SetErrror(err)
		return response
	}
	for _, result := range results {
		if count, err := result.RowsAffected();err == nil {
			response.RowsAffected += int(count)
		}
	}
	return response
}



func (s *service) RunScript(request *RunScriptRequest) *RunSQLResponse {
	var response = &RunSQLResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	if len(request.Scripts) == 0 {
		return response
	}
	var SQLs = []string{}
	var err error
	var storageService storage.Service
	var storageObject storage.Object
	for _, resource := range request.Scripts {
		var reader io.ReadCloser
		if storageService, err = storage.NewServiceForURL(resource.URL, resource.Credential); err == nil {
			if storageObject, err = storageService.StorageObject(resource.URL); err == nil {
				if reader, err = storageService.Download(storageObject); err == nil {
					defer reader.Close()
					SQLs = append(SQLs, script.ParseSQLScript(reader)...)
				}
			}
		}
		if err != nil {
			response.SetErrror(err)
			return response
		}
	}
	return s.RunSQLs(&RunSQLRequest{
		Datastore: request.Datastore,
		SQLs:      SQLs,
	})
}


func (s *service) AddTableMapping(request *MappingRequest) *MappingResponse {
	var response = &MappingResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
		Tables:       make([]string, 0),
	}
	if len(request.Mappings) == 0 {
		return response
	}
	for _, mapping := range request.Mappings {
		s.mapper.Add(mapping)
		response.Tables = append(response.Tables, mapping.Tables()...)
	}
	return response
}




func (s *service) Prepare(request *PrepareRequest) *PrepareResponse {
	return nil

}

func (s *service) Expect(request *ExpectRequest) *ExpectResponse {
	return nil

}

func (s *service) QueryExpect(request *QueryExpectRequest) *ExpectResponse {
	return nil

}



func New() Service {
	return &service{
		registry: dsc.NewManagerRegistry(),
		mapper:   NewMapper(),
	}
}
