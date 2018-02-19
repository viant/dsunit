package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/dsunit/script"
	"github.com/viant/toolbox/storage"
	"io"
	"github.com/viant/toolbox"
	"fmt"
	"github.com/viant/assertly"
)

var batchSize = 200

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

}




type service struct {
	registry      dsc.ManagerRegistry
	mapper        *Mapper
}

func (s *service) Registry() dsc.ManagerRegistry {
	return s.registry
}

func (s *service) Register(request *RegisterRequest) *RegisterResponse {
	var response = &RegisterResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	if request.RecreateDatastore && request.AdminDatastore == "" {
		request.AdminDatastore = request.Datastore
	}
	config := expandDscConfig(request.Config, request.Datastore)
	manager, err := dsc.NewManagerFactory().Create(config);
	if err == nil {
		s.registry.Register(request.Datastore, manager)
		if request.AdminDatastore != "" {
			adminConfig := request.AdminConfig
			if adminConfig == nil {
				adminConfig = request.Config
			}
			var adminManager dsc.Manager
			adminConfig = expandDscConfig(adminConfig, request.AdminDatastore)
			if adminManager, err = dsc.NewManagerFactory().Create(config); err == nil {
				s.registry.Register(request.AdminDatastore, adminManager)
				err = RecreateDatastore(request.AdminDatastore, request.Datastore, s.registry)
			}
		}
		if len(request.Descriptors) > 0 {
			for _, table:= range request.Descriptors {
				manager.TableDescriptorRegistry().Register(table)
			}
		}
	}

	if err != nil {
		response.SetErrror(err)
	}
	return response
}

func (s *service) RunSQLs(request *RunSQLRequest) *RunSQLResponse {
	var response = &RunSQLResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}

	manager := s.registry.Get(request.Datastore)
	results, err := manager.ExecuteAll(request.SQLs)
	if err != nil {
		response.SetErrror(err)
		return response
	}
	for _, result := range results {
		if count, err := result.RowsAffected(); err == nil {
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

func (s *service) newContext(manager dsc.Manager) toolbox.Context {
	context := toolbox.NewContext()

	return context
}

func (s *service) deleteDatasetIfNeeded(dataset *Dataset, table *dsc.TableDescriptor, response *PrepareResponse, context toolbox.Context, manager dsc.Manager, connection dsc.Connection) (err error) {
	if dataset.Records.ShouldDeleteAll() {
		dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
		sqlResult, err := manager.ExecuteOnConnection(connection, fmt.Sprintf("DELETE FROM %s", table.Table), nil)
		if err != nil {
			return err
		}
		deleted, _ := sqlResult.RowsAffected()
		response.Modification[dataset.Table].Deleted = int(deleted)
		//since deletion has to happen before new entries are added to address new modification, deletion needs to be committed first
		//for classified as insertable or updatable to work correctly
		connection.Commit()
		connection.Begin()
		dialect.DisableForeignKeyCheck(manager, connection)
	}
	return nil
}

func (s *service) populate(dataset *Dataset, response *PrepareResponse, context toolbox.Context, manager dsc.Manager, connection dsc.Connection) (err error) {
	if s.mapper.Has(dataset.Table) {
		datasets := s.mapper.Map(dataset)
		for _, dataset := range datasets {
			s.populate(dataset, response, context, manager, connection)
		}
		return
	}
	response.Modification[dataset.Table] = &ModificationInfo{Subject: dataset.Table, Method: "persist",}
	var modification = response.Modification[dataset.Table]
	var table *dsc.TableDescriptor
	if table, err = getTableDescriptor(dataset, manager, context); err != nil {
		return err
	}
	if err = s.deleteDatasetIfNeeded(dataset, table, response, context, manager, connection); err != nil {
		return err
	}
	var records []interface{}
	if records, err = dataset.Records.Expand(context); err != nil {
		return err
	}
	var dmlBuilder = newDatasetDmlProvider(dsc.NewDmlBuilder(table))
	if len(table.PkColumns) == 0 { //no keys perform insert
		modification.Method = "load"
		modification.Added, err = manager.PersistData(connection, records, table.Table, nil, insertSQLProvider(dmlBuilder)); //TODO add insert sql provider
		return err
	}
	modification.Added, modification.Modified, err = manager.PersistAllOnConnection(connection, &records, table.Table, dmlBuilder)
	return err
}

func (s *service) prepare(request *PrepareRequest, response *PrepareResponse, manager dsc.Manager, connection dsc.Connection) {
	err := connection.Begin()
	if err != nil {
		response.SetErrror(err)
	}

	context := s.newContext(manager)
	for _, dataset := range request.Datasets {
		s.populate(dataset, response, context, manager, connection)
	}
	if err == nil {
		err = connection.Commit()
	} else {
		_ = connection.Rollback()
	}
	if err != nil {
		response.SetErrror(err)
	}
}

func (s *service) Prepare(request *PrepareRequest) *PrepareResponse {
	var response = &PrepareResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	var err error
	var connection dsc.Connection
	manager := s.registry.Get(request.Datastore)
	if err = request.Load(); err == nil {
		connection, err = manager.ConnectionProvider().Get()
	}
	if err != nil {
		response.SetErrror(err)
		return response
	}
	dialect := GetDatastoreDialect(request.Datastore, s.registry)
	dialect.DisableForeignKeyCheck(manager, connection)
	defer dialect.EnableForeignKeyCheck(manager, connection)
	defer connection.Close()
	s.prepare(request, response, manager, connection)
	return response

}

func (s *service) expect(policy int, dataset *Dataset, response *ExpectResponse, context toolbox.Context, manager dsc.Manager) (err error) {
	if s.mapper.Has(dataset.Table) {
		datasets := s.mapper.Map(dataset)
		for _, dataset := range datasets {
			s.expect(policy, dataset, response, context, manager)
		}
		return nil
	}
	var table *dsc.TableDescriptor
	if table, err = getTableDescriptor(dataset, manager, context); err != nil {
		return err
	}
	if _, err = dataset.Records.Expand(context); err != nil {
		return err
	}
	expected := dataset.Records
	var columns = dataset.Records.Columns()
	var mapper = newDatasetRowMapper(columns)
	var parametrizedSQL *dsc.ParametrizedSQL

	sqlBuilder := dsc.NewQueryBuilder(table, "")
	var actual = make([]interface{}, 0)
	var validation = &DatasetValidation{
		Dataset: dataset.Table,
	}
	if policy == FullTableDatasetCheckPolicy || len(table.PkColumns) == 0 { //no keys perform insert
		parametrizedSQL = sqlBuilder.BuildQueryAll(columns)
		if err = manager.ReadAll(&actual, parametrizedSQL.SQL, parametrizedSQL.Values, mapper); err != nil {
			return err
		}
	} else {
		pkValues := buildBatchedPkValues(expected, table.PkColumns)
		for _, parametrizedSQL = range sqlBuilder.BuildBatchedQueryOnPk(columns, pkValues, batchSize) {
			var batched = make([]interface{}, 0)
			err := manager.ReadAll(&batched, parametrizedSQL.SQL, parametrizedSQL.Values, mapper)
			if err != nil {
				return err
			}
			actual = append(actual, batched...)
		}
	}
	if validation.Validation, err = assertly.Assert(expected, actual, assertly.NewDataPath(table.Table)); err == nil {
		response.Validation = append(response.Validation, validation)
		response.FailedCount += validation.Validation.FailedCount
		response.PassedCount += validation.Validation.PassedCount
	}
	return err
}



func (s *service) Expect(request *ExpectRequest) *ExpectResponse {
	var response = &ExpectResponse{
		BaseResponse: &BaseResponse{Status: "ok"},
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	context := s.newContext(manager)
	var err error
	if err = request.Load(); err == nil {
		for _, dataset := range request.Datasets {
			s.expect(request.CheckPolicy, dataset, response, context, manager)
		}
	}
	response.SetErrror(err)
	return response
}




//New creates new dsunit service
func New() Service {
	fmt.Printf("bootstrap service with %v\n", baseDirectory)
	return &service{
		registry:      dsc.NewManagerRegistry(),
		mapper:        NewMapper(),
	}
}




//GetDatastoreDialect return GetDatastoreDialect for supplied datastore and registry.
func GetDatastoreDialect(datastore string, registry dsc.ManagerRegistry) dsc.DatastoreDialect {
	manager := registry.Get(datastore)
	dbConfig := manager.Config()
	return dsc.GetDatastoreDialect(dbConfig.DriverName)
}

//RecreateDatastore recreates target datastore from supplied admin datastore and registry
func RecreateDatastore(adminDatastore, targetDatastore string, registry dsc.ManagerRegistry) error {
	dialect := GetDatastoreDialect(adminDatastore, registry)
	adminManager := registry.Get(adminDatastore)
	if !dialect.CanDropDatastore(adminManager) {
		return recreateTables(registry, targetDatastore)
	}
	return recreateDatastore(adminManager, registry, targetDatastore)
}
