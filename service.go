package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/dsunit/script"
	"github.com/viant/toolbox/storage"
	"io"
	"github.com/viant/toolbox"
	"fmt"
	"github.com/viant/assertly"
	"github.com/pkg/errors"
	"github.com/viant/toolbox/data"
)

var batchSize = 200

//Service represents test service
type Service interface {
	//Registry returns registry of registered database managers
	Registry() dsc.ManagerRegistry

	//Register registers new datastore connection
	Register(request *RegisterRequest) *RegisterResponse

	//Recreate remove and creates datastore
	Recreate(request *RecreateRequest) *RecreateResponse

	//RunSQL runs supplied SQL
	RunSQL(request *RunSQLRequest) *RunSQLResponse

	//RunScript runs supplied SQL scripts
	RunScript(request *RunScriptRequest) *RunSQLResponse

	//Add table mapping
	AddTableMapping(request *MappingRequest) *MappingResponse

	//Init datastore, (register, recreated, run sql, add mapping)
	Init(request *InitRequest) *InitResponse

	//Populate database with datasets
	Prepare(request *PrepareRequest) *PrepareResponse

	//Verify datastore with supplied expected datasets
	Expect(request *ExpectRequest) *ExpectResponse

	//Query returns query from database
	Query(request *QueryRequest) *QueryResponse

	//Sequence returns sequence for supplied tables
	Sequence(request *SequenceRequest) *SequenceResponse

	SetContext(context toolbox.Context)
}

type service struct {
	registry dsc.ManagerRegistry
	mapper   *Mapper
	context  toolbox.Context
}

func (s *service) Registry() dsc.ManagerRegistry {
	return s.registry
}

func (s *service) Register(request *RegisterRequest) *RegisterResponse {
	var err error
	var response = &RegisterResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	if request.ConfigURL != "" {
		if request.Config, err = dsc.NewConfigFromURL(request.ConfigURL); err != nil {
			response.SetError(err)
			return response
		}
	}
	config, err := expandDscConfig(request.Config, request.Datastore)
	if err != nil {
		response.SetError(err)
		return response
	}
	manager, err := dsc.NewManagerFactory().Create(config);
	if err == nil {
		s.registry.Register(request.Datastore, manager)
		if len(request.Tables) > 0 {
			for _, table := range request.Tables {
				manager.TableDescriptorRegistry().Register(table)
			}
		}
	}
	if err != nil {
		response.SetError(err)
	}
	return response
}

//Recreate removes and re-creates datastore
func (s *service) Recreate(request *RecreateRequest) *RecreateResponse {
	var response = &RecreateResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	if request.AdminDatastore == "" {
		request.AdminDatastore = request.Datastore
	}
	err := RecreateDatastore(request.AdminDatastore, request.Datastore, s.registry)
	response.SetError(err)
	return response
}

//expandSQLIfNeeded expand content of SQL with context.state key
func (s *service) expandSQLIfNeeded(request *RunSQLRequest, manager dsc.Manager) []string {
	if ! request.Expand {
		return request.SQL
	}
	context := s.newContext(manager)
	state := s.getContextState(context)
	if state == nil {
		return request.SQL
	}
	result := make([]string, 0)
	for _, SQL := range request.SQL {
		result = append(result, state.ExpandAsText(SQL))
	}
	return result
}

func (s *service) RunSQL(request *RunSQLRequest) *RunSQLResponse {
	var response = &RunSQLResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}

	manager := s.registry.Get(request.Datastore)
	var SQL = s.expandSQLIfNeeded(request, manager)
	results, err := manager.ExecuteAll(SQL)
	if err != nil {
		response.SetError(err)
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
		BaseResponse: NewBaseOkResponse(),
	}
	if len(request.Scripts) == 0 {
		return response
	}
	var SQL = []string{}
	var err error
	var storageService storage.Service
	var storageObject storage.Object
	for _, resource := range request.Scripts {
		resource.Init()
		var reader io.ReadCloser
		if storageService, err = storage.NewServiceForURL(resource.URL, resource.Credential); err == nil {
			if storageObject, err = storageService.StorageObject(resource.URL); err == nil {
				if reader, err = storageService.Download(storageObject); err == nil {
					defer reader.Close()
					SQL = append(SQL, script.ParseSQLScript(reader)...)
				}
			}
		}
		if err != nil {
			response.SetError(err)
			return response
		}
	}
	return s.RunSQL(&RunSQLRequest{
		Expand:    request.Expand,
		Datastore: request.Datastore,
		SQL:       SQL,
	})
}

func (s *service) AddTableMapping(request *MappingRequest) *MappingResponse {
	var response = &MappingResponse{
		BaseResponse: NewBaseOkResponse(),
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

//Init datastore, (register, recreated, run sql, add mapping)
func (s *service) Init(request *InitRequest) *InitResponse {
	var response = &InitResponse{BaseResponse: NewBaseOkResponse()}
	if request.Datastore == "" {
		response.SetError(errors.New("datastore was empty"))
		return response
	}
	registerRequest := request.RegisterRequest
	if registerRequest == nil {
		response.SetError(errors.New("unable recreates - registerRequest datastore was empty"))
		return response
	}
	if registerRequest.Datastore == "" {
		registerRequest.Datastore = request.Datastore
	}

	registerRequests := []*RegisterRequest{registerRequest, request.Admin}
	for _, registerRequest := range registerRequests {
		if registerRequest == nil {
			continue
		}
		serviceResponse := s.Register(registerRequest)
		if serviceResponse.Status != StatusOk {
			response.BaseResponse = serviceResponse.BaseResponse
			return response
		}
	}

	if request.Recreate {
		var adminDatastore = registerRequest.Datastore
		if request.Admin != nil {
			adminDatastore = request.Admin.Datastore
		}
		serviceResponse := s.Recreate(NewRecreateRequest(registerRequest.Datastore, adminDatastore))
		if serviceResponse.Status != StatusOk {
			response.BaseResponse = serviceResponse.BaseResponse
			return response
		}
	}

	if request.RunScriptRequest != nil && len(request.Scripts) > 0 {
		if request.RunScriptRequest.Datastore == "" {
			request.RunScriptRequest.Datastore = request.Datastore
		}
		serviceResponse := s.RunScript(request.RunScriptRequest)
		if serviceResponse.Status != StatusOk {
			response.BaseResponse = serviceResponse.BaseResponse
			return response
		}
	}

	if request.MappingRequest != nil && len(request.Mappings) > 0 {
		serviceResponse := s.AddTableMapping(request.MappingRequest)
		if serviceResponse.Status != StatusOk {
			response.BaseResponse = serviceResponse.BaseResponse
			return response
		}
		response.Tables = serviceResponse.Tables
	}
	return response
}

var stateKey = (*data.Map)(nil)

func (s *service) getContextState(context toolbox.Context) *data.Map {
	if ! context.Contains(stateKey) {
		return nil
	}
	var state = context.GetOptional(stateKey).(*data.Map)
	return state
}

func (s *service) newContext(manager dsc.Manager) toolbox.Context {
	context := toolbox.NewContext()
	if s.context != nil {
		context = s.context.Clone()
	}
	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	context.Replace((*dsc.Manager)(nil), &manager)
	context.Replace((*dsc.DatastoreDialect)(nil), &dialect)
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

func (s *service) getTableDescriptor(dataset *Dataset, manager dsc.Manager, context toolbox.Context) (*dsc.TableDescriptor, error) {
	macroEvaluator := assertly.NewDefaultMacroEvaluator()
	expandedTable, err := macroEvaluator.Expand(context, dataset.Table)
	if err != nil {
		return nil, err
	}
	var state = s.getContextState(context)
	tableName := state.ExpandAsText(toolbox.AsString(expandedTable))
	table := manager.TableDescriptorRegistry().Get(tableName)
	if table == nil {
		table = &dsc.TableDescriptor{Table: tableName}
		manager.TableDescriptorRegistry().Register(table)
	}
	var autoincrement = dataset.Records.Autoincrement()
	var uniqueKeys = dataset.Records.UniqueKeys()
	var fromQuery = dataset.Records.FromQuery()
	if ! table.Autoincrement {
		table.Autoincrement = autoincrement
	}
	table.FromQuery = fromQuery
	if len(table.PkColumns) == 0 {
		table.PkColumns = uniqueKeys
	} else if len(uniqueKeys) == 0 {
		if len(dataset.Records) > 0 {
			if len(dataset.Records[0]) == 0 {
				dataset.Records =
					append([]map[string]interface{}{{
						assertly.IndexByDirective: table.PkColumns,
					},
					}, dataset.Records...)

			} else {
				dataset.Records[0][assertly.IndexByDirective] = table.PkColumns
			}
		}
	}
	var columns = dataset.Records.Columns()
	if len(columns) > 0 {
		table.Columns = columns
	}
	return table, nil
}

func (s *service) populate(dataset *Dataset, response *PrepareResponse, context toolbox.Context, manager dsc.Manager, connection dsc.Connection) (err error) {
	if s.mapper.Has(dataset.Table) {
		datasets := s.mapper.Map(dataset)
		for _, dataset := range datasets {
			if err = s.populate(dataset, response, context, manager, connection); err != nil {
				return err
			}
		}
		return
	}
	if len(response.Modification) == 0 {
		response.Modification = make(map[string]*ModificationInfo)
	}
	response.Modification[dataset.Table] = &ModificationInfo{Subject: dataset.Table, Method: "persist",}
	var modification = response.Modification[dataset.Table]
	var table *dsc.TableDescriptor
	if table, err = s.getTableDescriptor(dataset, manager, context); err != nil {
		return err
	}

	if err = s.deleteDatasetIfNeeded(dataset, table, response, context, manager, connection); err != nil {
		return err
	}
	context.Replace((*Dataset)(nil), dataset)
	context.Replace((*dsc.TableDescriptor)(nil), table)
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
		response.SetError(err)
	}

	context := s.newContext(manager)
	for _, dataset := range request.Datasets {
		err = s.populate(dataset, response, context, manager, connection)
		if err != nil {
			break
		}
	}
	if err == nil {
		err = connection.Commit()
	} else {
		_ = connection.Rollback()
	}
	if err != nil {
		response.SetError(err)
	}
}

func (s *service) Prepare(request *PrepareRequest) *PrepareResponse {
	var response = &PrepareResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	if err := request.Validate(); err != nil {
		response.SetError(err)
		return response
	}

	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	var err error
	var connection dsc.Connection
	manager := s.registry.Get(request.Datastore)
	if err = request.Load(); err == nil {
		if len(request.Datasets) == 0 {
			response.SetError(fmt.Errorf("no dataset: %v/%v", request.URL, request.Prefix+"*"+request.Postfix))
			return response
		}
		connection, err = manager.ConnectionProvider().Get()
	}
	if err != nil {
		response.SetError(err)
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
			if err = s.expect(policy, dataset, response, context, manager); err != nil {
				return err
			}
		}
		return err
	}

	var table *dsc.TableDescriptor
	if table, err = s.getTableDescriptor(dataset, manager, context); err != nil {
		return err
	}
	context.Replace((*Dataset)(nil), dataset)
	context.Replace((*dsc.TableDescriptor)(nil), table)

	expectedRecords, err := dataset.Records.Expand(context);
	if err != nil {
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

	if validation.Validation, err = assertly.Assert(expectedRecords, actual, assertly.NewDataPath(table.Table)); err == nil {
		response.Validation = append(response.Validation, validation)
		response.FailedCount += validation.Validation.FailedCount
		response.PassedCount += validation.Validation.PassedCount
		response.Message += "\n" + dataset.Table + "\n" + validation.Report()
	}
	return err
}

func (s *service) Expect(request *ExpectRequest) *ExpectResponse {
	var response = &ExpectResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	if err := request.Validate(); err != nil {
		response.SetError(err)
		return response
	}

	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	context := s.newContext(manager)
	var err error

	if err = request.Load(); err == nil {
		if len(request.Datasets) == 0 {
			response.SetError(fmt.Errorf("no dataset: %v/%v", request.URL, request.Prefix+"*"+request.Postfix))
			return response
		}
		for _, dataset := range request.Datasets {
			if err = s.expect(request.CheckPolicy, dataset, response, context, manager); err != nil {
				break
			}
		}

	}

	response.SetError(err)
	return response
}

//Query returns query from database
func (s *service) Query(request *QueryRequest) *QueryResponse {
	var response = &QueryResponse{
		BaseResponse: NewBaseOkResponse(),
		Records:      make([]map[string]interface{}, 0),
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	macroEvaluator := assertly.NewDefaultMacroEvaluator()
	context := toolbox.NewContext()
	SQL, err := macroEvaluator.Expand(context, request.SQL)
	if err != nil {
		response.SetError(err)
		return response
	}

	err = manager.ReadAll(&response.Records, toolbox.AsString(SQL), nil, nil)
	response.SetError(err)
	return response
}

//Sequence returns sequence for supplied tables
func (s *service) Sequence(request *SequenceRequest) *SequenceResponse {
	var response = &SequenceResponse{
		BaseResponse: NewBaseOkResponse(),
		Sequences:    make(map[string]int),
	}
	if len(request.Tables) == 0 {
		response.SetError(errors.New("tables were empty"))
	}
	if ! validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	dialect := GetDatastoreDialect(request.Datastore, s.registry)
	for _, table := range request.Tables {
		if sequence, err := dialect.GetSequence(manager, table); err == nil {
			response.Sequences[table] = int(sequence)
		}
	}
	return response
}

func (s *service) SetContext(context toolbox.Context) {
	s.context = context
}

//New creates new dsunit service
func New() Service {
	return &service{
		registry: dsc.NewManagerRegistry(),
		mapper:   NewMapper(),
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
