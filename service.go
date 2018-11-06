package dsunit

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"github.com/viant/dsunit/script"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/url"
	"io"
	"strings"
)

var batchSize = 200

//SubstitutionMapKey if provided in context, it will be used to substitute/expand dataset
var SubstitutionMapKey = (*data.Map)(nil)

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

	//Freeze creates a dataset from existing database/datastore (reverse engineering test setup/verification)
	Freeze(request *FreezeRequest) *FreezeResponse

	//Dump creates a database schema from existing database for supplied tables, datastore
	Dump(request *DumpRequest) *DumpResponse

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
	var response = &RegisterResponse{
		BaseResponse: NewBaseOkResponse(),
	}
	var err = request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		response.SetError(err)
		return response
	}
	config, err := expandDscConfig(request.Config, request.Datastore)
	if err != nil {
		response.SetError(err)
		return response
	}
	manager, err := dsc.NewManagerFactory().Create(config)
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
	if !request.Expand {
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

	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
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
		count, err := result.RowsAffected()
		if err != nil {
			response.SetError(err)
			return response
		}
		response.RowsAffected += int(count)
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
		if storageService, err = storage.NewServiceForURL(resource.URL, resource.Credentials); err == nil {
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
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		response.SetError(err)
		return response
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
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		response.SetError(err)
		return response
	}
	registerRequest := request.RegisterRequest
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

func (s *service) getContextState(context toolbox.Context) *data.Map {
	if !context.Contains(SubstitutionMapKey) {
		return nil
	}
	var state = context.GetOptional(SubstitutionMapKey).(*data.Map)
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
	var fromQuery, fromQueryAlias = dataset.Records.FromQuery()
	if !table.Autoincrement {
		table.Autoincrement = autoincrement
	}
	table.FromQuery = fromQuery
	table.FromQueryAlias = fromQueryAlias
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

	response.Modification[dataset.Table] = &ModificationInfo{Subject: dataset.Table, Method: "persist"}
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
	expandDataIfNeeded(context, dataset.Records)
	if records, err = dataset.Records.Expand(context, false); err != nil {
		return err
	}
	var dmlBuilder = newDatasetDmlProvider(dsc.NewDmlBuilder(table))
	if len(table.PkColumns) == 0 { //no keys perform insert
		modification.Method = "load"
		modification.Added, err = manager.PersistData(connection, records, table.Table, nil, insertSQLProvider(dmlBuilder)) //TODO add insert sql provider
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
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		response.SetError(err)
		return response
	}
	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
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

	expandDataIfNeeded(context, dataset.Records)
	expectedRecords, err := dataset.Records.Expand(context, true)
	if err != nil {
		return err
	}

	expected := dataset.Records
	var columns = dataset.Records.Columns()

	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	datastore, _ := dialect.GetCurrentDatastore(manager)

	var types []dsc.Column

	if table.FromQuery == "" {
		types, _ = dialect.GetColumns(manager, datastore, table.Table)
	}
	var mapper = newDatasetRowMapper(columns, types)
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

	validation.Expected = expectedRecords
	validation.Actual = actual
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
	err := request.Init()
	if err == nil {
		err = request.Validate()
	}
	if err != nil {
		response.SetError(err)
		return response
	}

	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	context := s.newContext(manager)

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
	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	manager := s.registry.Get(request.Datastore)
	macroEvaluator := assertly.NewDefaultMacroEvaluator()
	context := toolbox.NewContext()
	state := s.getContextState(context)
	SQL, err := macroEvaluator.Expand(context, request.SQL)
	if err != nil {
		response.SetError(err)
		return response
	}
	if state != nil {
		SQL = state.Expand(toolbox.AsString(SQL))
	}
	err = manager.ReadAll(&response.Records, toolbox.AsString(SQL), nil, nil)
	response.SetError(err)
	return response
}

//Freeze creates a dataset from dataset (reverse engineering test setup/verification)
func (s *service) Freeze(request *FreezeRequest) *FreezeResponse {
	var response = &FreezeResponse{BaseResponse: NewBaseOkResponse()}
	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
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
	var records = make([]map[string]interface{}, 0)
	err = manager.ReadAll(&records, toolbox.AsString(SQL), nil, nil)
	if err != nil {
		response.SetError(err)
		return response
	}
	destResource := url.NewResource(request.DestURL)
	if len(records) > 0 {
		for i, record := range records {
			if request.OmitEmpty {
				records[i] = toolbox.DeleteEmptyKeys(record)
			}
			if len(request.Ignore) > 0 {
				var record = data.Map(records[i])
				for _, path := range request.Ignore {
					record.Delete(path)
				}
				records[i] = record
			}
			if len(request.Replace) > 0 {
				var record = data.Map(records[i])
				for k, v := range request.Replace {
					record.Replace(k, escapeVariableIfNeeded(v))
				}
				records[i] = record
			}
		}
	}
	payload, err := toolbox.AsIndentJSONText(records)
	if err != nil {
		response.SetError(err)
		return response
	}
	response.Count = len(records)
	response.DestURL = destResource.URL
	uploadContent(destResource, response.BaseResponse, []byte(payload))
	return response
}

//Dump creates a database schema from existing database
func (s *service) Dump(request *DumpRequest) *DumpResponse {
	var response = &DumpResponse{BaseResponse: NewBaseOkResponse()}
	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
		return response
	}
	var err error
	manager := s.registry.Get(request.Datastore)
	dialect := dsc.GetDatastoreDialect(manager.Config().DriverName)
	tables := request.Tables
	if len(tables) == 0 {
		if tables, err = dialect.GetTables(manager, request.Datastore); err != nil {
			response.SetError(err)
			return response
		}
	}
	destResource := url.NewResource(request.DestURL)
	var DDLs = []string{}
	for _, table := range tables {
		ddl, err := dialect.ShowCreateTable(manager, table)
		if err != nil {
			response.SetError(err)
			return response
		}
		DDLs = append(DDLs, ddl)
	}
	var payload = strings.Join(DDLs, "\n\n")
	response.Count = len(DDLs)
	response.DestURL = destResource.URL
	uploadContent(destResource, response.BaseResponse, []byte(payload))
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
	if !validateDatastores(s.registry, response.BaseResponse, request.Datastore) {
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
	var err error
	if err = recreateDatastore(adminManager, registry, targetDatastore); err == nil {
		err = recreateTables(registry, targetDatastore)
	}
	return err
}
