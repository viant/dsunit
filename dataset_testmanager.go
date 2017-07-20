package dsunit

import (
	"fmt"
	"strings"

	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

var batchSize = 200

//DatasetTestManager represetns manager that manages prepareation and verification a datastore with datasets.
type datasetTestManager struct {
	managerRegistry        dsc.ManagerRegistry
	valueProviderRegistry  toolbox.ValueProviderRegistry
	macroEvaluator         *toolbox.MacroEvaluator
	datasetFactory         DatasetFactory
	datasetMappingRegistry *datasetTransformerRegistry
}

//GetDialectable return DatastoreDialect for passed in driver.
func (tm *datasetTestManager) GetDialectable(datastore string) dsc.DatastoreDialect {
	manager := tm.managerRegistry.Get(datastore)
	dbConfig := manager.Config()
	return dsc.GetDatastoreDialect(dbConfig.DriverName)
}

func (tm *datasetTestManager) dropDatastoreIfNeeded(adminDatastore string, targetDatastore string) error {
	if !strings.Contains(targetDatastore, "test") {
		return dsUnitError{("Faild to recreate datastore: " + targetDatastore + " - Only test datastore can be recreated (databse name has to contain 'test' fragment)")}
	}
	adminManager := tm.managerRegistry.Get(adminDatastore)
	dialect := tm.GetDialectable(adminDatastore)
	existingDatastores, err := dialect.GetDatastores(adminManager)
	if err != nil {
		return err
	}
	hasDatastore := toolbox.HasSliceAnyElements(existingDatastores, targetDatastore)
	if hasDatastore {
		err = dialect.DropDatastore(adminManager, targetDatastore)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (tm *datasetTestManager) recreateTables(adminDatastore string, targetDatastore string) error {
	adminManager := tm.managerRegistry.Get(adminDatastore)
	dialect := tm.GetDialectable(adminDatastore)

	tables, err := dialect.GetTables(adminManager, targetDatastore)
	if err != nil {
		return err
	}
	var existingTables = make(map[string]bool)
	toolbox.SliceToMap(tables, existingTables, toolbox.CopyStringValueProvider, toolbox.TrueValueProvider)
	targetManager := tm.managerRegistry.Get(targetDatastore)
	tableRegistry := targetManager.TableDescriptorRegistry()
	for _, table := range tableRegistry.Tables() {
		if _, found := existingTables[table]; found {

			err := dialect.DropTable(adminManager, targetDatastore, table)
			if err != nil {
				return err
			}

		}
		descriptor := tableRegistry.Get(table)
		if descriptor.HasSchema() {
			err := dialect.CreateTable(targetManager, targetDatastore, table, "")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *datasetTestManager) recreateDatastore(adminDatastore string, targetDatastore string) error {
	adminManager := tm.managerRegistry.Get(adminDatastore)
	dialect := tm.GetDialectable(adminDatastore)
	err := tm.dropDatastoreIfNeeded(adminDatastore, targetDatastore)
	if err != nil {
		return err
	}

	err = dialect.CreateDatastore(adminManager, targetDatastore)
	if err != nil {
		return err
	}
	return nil
}

//ClearDatastore clears datastore, it takes adminDatastore and targetDatastore names.
func (tm *datasetTestManager) ClearDatastore(adminDatastore string, targetDatastore string) error {
	adminManager := tm.managerRegistry.Get(adminDatastore)
	dialect := tm.GetDialectable(adminDatastore)
	if !dialect.CanDropDatastore(adminManager) {
		return tm.recreateTables(adminDatastore, targetDatastore)
	}
	err := tm.recreateDatastore(adminDatastore, targetDatastore)
	if err != nil {
		return err
	}
	return nil

}

func (tm *datasetTestManager) dropDatastore(adminDatastore string, targetDatastore string) error {
	err := tm.dropDatastoreIfNeeded(adminDatastore, targetDatastore)
	if err != nil {
		return err
	}
	return nil

}

//Execute executes passed in script, script defines what database it run on.
func (tm *datasetTestManager) Execute(script *Script) (int, error) {
	scriptManager := tm.managerRegistry.Get(script.Datastore)

	if len(script.Body) > 0 && len(script.Sqls) == 0 {
		reader := strings.NewReader(script.Body)
		result := ParseSQLScript(reader)
		script.Sqls = result
	}
	results, err := scriptManager.ExecuteAll(script.Sqls)
	if err != nil {
		return 0, err
	}
	affected := 0
	for _, result := range results {
		count, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		affected = affected + int(count)
	}

	return affected, err
}

//ExecuteFromURL reads content from url and executes it on datastore
func (tm *datasetTestManager) ExecuteFromURL(datastore string, url string) (int, error) {
	reader, _, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	result := ParseSQLScript(reader)
	script := Script{
		Datastore: datastore,
		Sqls:      result,
	}
	return tm.Execute(&script)
}

func getSQLProvider(dataset *Dataset, row *Row) dsc.DmlProvider {
	descriptor := &dsc.TableDescriptor{
		Table:         dataset.Table,
		PkColumns:     dataset.PkColumns,
		Autoincrement: dataset.Autoincrement,
		Columns:       (*row).Columns(),
	}
	dmlBuilder := dsc.NewDmlBuilder(descriptor)
	return newDatasetDmlProvider(dmlBuilder)
}

func (tm *datasetTestManager) persistDataset(connection dsc.Connection, manager dsc.Manager, dataset *Dataset) (inserted int, updated int, err error) {
	var rows = make([]*Row, 0)
	for i, row := range dataset.Rows {
		rows = append(rows, row)
		key := strings.Join(toolbox.SortStrings(toolbox.MapKeysToStringSlice(row.Values)), ",")
		nextKey := ""
		if i+1 < len(dataset.Rows) {
			nextRow := dataset.Rows[i+1]
			nextKey = strings.Join(toolbox.SortStrings(toolbox.MapKeysToStringSlice(nextRow.Values)), ",")
		}
		if key == nextKey {
			continue
		}

		added, changed, err := manager.PersistAllOnConnection(connection, &rows, dataset.Table, getSQLProvider(dataset, row))
		if err != nil {
			return 0, 0, err
		}
		inserted += added
		updated += changed
		rows = make([]*Row, 0)

	}
	return inserted, updated, nil
}

func (tm *datasetTestManager) persistDatasetInBatch(connection dsc.Connection, manager dsc.Manager, dataset *Dataset) (inserted int, updated int, err error) {
	var rows = make([]*Row, 0)
	mergedRow := Row{
		Values: make(map[string]interface{}),
	}
	for _, row := range dataset.Rows {
		rows = append(rows, row)
		for key, value := range row.Values {
			mergedRow.Values[key] = value
		}
	}
	return manager.PersistAllOnConnection(connection, &rows, dataset.Table, getSQLProvider(dataset, &mergedRow))

}

func (tm *datasetTestManager) prepareDatasets(datastore string, datasets *[]*Dataset, context toolbox.Context, manager dsc.Manager, connection dsc.Connection) (inserted, updated, deleted int, err error) {
	var insertedTotal, updatedTotal, deletedTotal int
	dialect := tm.GetDialectable(datastore)
	for _, dataset := range *datasets {
		err := tm.expandTable(dataset)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("Failed to prepare datastore %v - unable to expand macro in the table %v due to %v", datastore, dataset.Table, err)
		}
		updateDatasetDescriptorIfNeeded(manager, dataset)
		err = tm.expandMacros(context, datastore, manager, dataset)
		if err != nil {
			return 0, 0, 0, fmt.Errorf("Failed to prepare datastore %v - unable to expand macros %v", datastore, err)
		}

		if tm.datasetMappingRegistry.has(dataset.Table) {
			transformer := NewDatasetTransformer()
			mapping := tm.datasetMappingRegistry.get(dataset.Table)
			registry := manager.TableDescriptorRegistry()
			mappedDatasets := transformer.Transform(datastore, dataset, mapping, registry)
			inserted, updated, deleted, err = tm.prepareDatasets(datastore, &mappedDatasets.Datasets, context, manager, connection)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("Failed to prepare datastore %v - unable to persist %v", datastore, err)
			}
			insertedTotal += inserted
			updatedTotal += updated
			continue
		}

		if dataset.Rows == nil || len(dataset.Rows) == 0 {
			result, err := manager.ExecuteOnConnection(connection, "DELETE FROM "+dataset.Table, nil)
			if err != nil {
				return 0, 0, 0, fmt.Errorf("Failed to prepare datastore %v - unable to delete table %v due to %v", datastore, dataset.Table, err)
			}
			affected, _ := result.RowsAffected()
			deletedTotal += int(affected)
		}

		if dialect.CanPersistBatch() {
			inserted, updated, err = tm.persistDatasetInBatch(connection, manager, dataset)
		} else {
			inserted, updated, err = tm.persistDataset(connection, manager, dataset)
		}
		if err != nil {
			return 0, 0, 0, fmt.Errorf("Failed to prepare datastore %v - unable to persist %v", datastore, err)
		}
		insertedTotal += inserted
		updatedTotal += updated
	}
	return insertedTotal, updatedTotal, deletedTotal, err

}

//PrepareDatastore prepare datastore datasets by adding, updating or deleting data.
// Rows will be added if they weren't present, updated if they were present, and deleted if passed in dataset has not rows defined.
func (tm *datasetTestManager) PrepareDatastore(datasets *Datasets) (inserted, updated, deleted int, err error) {
	manager := tm.managerRegistry.Get(datasets.Datastore)
	connection, err := manager.ConnectionProvider().Get()
	if err != nil {
		return inserted, updated, deleted, err
	}
	defer connection.Close()

	context := toolbox.NewContext()

	err = connection.Begin()
	if err != nil {
		return 0, 0, 0, fmt.Errorf("Failed to start transaction on %v due to %v", manager.Config().Descriptor, err)
	}
	inserted, updated, deleted, err = tm.prepareDatasets(datasets.Datastore, &datasets.Datasets, context, manager, connection)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return 0, 0, 0, fmt.Errorf("Failed to commit on %v due to %v", manager.Config().Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return 0, 0, 0, fmt.Errorf("Failed to rollback on %v due to %v, %v", manager.Config().Descriptor, err, rollbackErr)
		}
	}
	return inserted, updated, deleted, err
}

func (tm *datasetTestManager) assertDatasets(datastore string, expected *Dataset, actual *Dataset) ([]AssertViolation, error) {
	datasetTester := &DatasetTester{}
	manager := tm.managerRegistry.Get(datastore)
	config := manager.Config()
	if config.HasDateLayout() {
		datasetTester.dateLayout = config.GetDateLayout()
	}
	violations := datasetTester.Assert(datastore, expected, actual)
	return violations, nil
}

func (tm *datasetTestManager) expectFullDatasets(manager dsc.Manager, datastore string, expected *Dataset, mapper dsc.RecordMapper) ([]AssertViolation, error) {
	config := manager.Config()
	queryHint := ""
	if config.Has("queryHint") {
		queryHint = config.Get("queryHint")
	}

	sqlBuilder := dsc.NewQueryBuilder(expected.TableDescriptor, queryHint)
	sqlWithArguments := sqlBuilder.BuildQueryAll(expected.Columns)
	var rows = make([]*Row, 0)
	err := manager.ReadAll(&rows, sqlWithArguments.SQL, sqlWithArguments.Values, mapper)
	if err != nil {
		return nil, err
	}
	actual := &Dataset{
		TableDescriptor: expected.TableDescriptor,
		Rows:            rows,
	}

	return tm.assertDatasets(datastore, expected, actual)
}

func buildPkValues(dataset *Dataset) [][]interface{} {
	var pkValues = make([][]interface{}, 0)
	for _, row := range dataset.Rows {
		var pkRow = make([]interface{}, 0)
		for _, pkColumn := range dataset.PkColumns {
			pkRow = append(pkRow, row.Value(pkColumn))
		}
		pkValues = append(pkValues, pkRow)
	}
	return pkValues
}

func (tm *datasetTestManager) expectSnapshotDatasets(manager dsc.Manager, datastore string, expected *Dataset, mapper dsc.RecordMapper) ([]AssertViolation, error) {
	var pkValues = buildPkValues(expected)
	var rows = make([]*Row, 0)
	config := manager.Config()
	queryHint := ""
	if config.Has("queryHint") {
		queryHint = config.Get("queryHint")
	}
	sqlBuilder := dsc.NewQueryBuilder(expected.TableDescriptor, queryHint)
	for _, sqlWithArguments := range sqlBuilder.BuildBatchedQueryOnPk(expected.Columns, pkValues, batchSize) {
		var batched = make([]*Row, 0)
		err := manager.ReadAll(&batched, sqlWithArguments.SQL, sqlWithArguments.Values, mapper)
		if err != nil {
			return nil, err
		}
		rows = append(rows, batched...)
	}
	actual := &Dataset{
		TableDescriptor: expected.TableDescriptor,
		Rows:            rows,
	}
	return tm.assertDatasets(datastore, expected, actual)
}

func (tm *datasetTestManager) expandMacro(context toolbox.Context, row *Row, column string, dataset *Dataset) error {
	value := (*row).Value(column)
	if textValue, ok := value.(string); ok {
		if tm.macroEvaluator.HasMacro(textValue) {
			expanded, err := tm.macroEvaluator.Expand(context, textValue)
			if err != nil {
				return dsUnitError{"Failed to expandMacro on " + dataset.Table + " " + (*row).String() + " due to:\n\t" + err.Error()}
			}
			(*row).SetValue(column, expanded)
		}
	}
	return nil
}

func (tm *datasetTestManager) expandMacros(context toolbox.Context, datastore string, manager dsc.Manager, dataset *Dataset) error {
	dialect := tm.GetDialectable(datastore)
	context.Replace((*Dataset)(nil), dataset)
	context.Replace((*dsc.Manager)(nil), &manager)
	context.Replace((*dsc.DatastoreDialect)(nil), &dialect)
	for _, row := range dataset.Rows {
		for _, column := range row.Columns() {
			err := tm.expandMacro(context, row, column, dataset)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *datasetTestManager) expandTable(dataset *Dataset) error {
	table, err := toolbox.ExpandValue(tm.macroEvaluator, dataset.Table)
	if err != nil {
		return err
	}
	dataset.Table = table
	return nil
}

func (tm *datasetTestManager) expandFromQuery(context toolbox.Context, dataset *Dataset) error {
	context.Replace((*Dataset)(nil), dataset)
	if len(dataset.TableDescriptor.FromQuery) > 0 {
		fromQuery, err := tm.macroEvaluator.Expand(context, dataset.TableDescriptor.FromQuery)
		if err != nil {
			return err
		}
		dataset.TableDescriptor.FromQuery = fromQuery.(string)
		dataset.FromQuery = fromQuery.(string)
	}
	return nil
}

func buildColumnsForDataset(dataset *Dataset) []string {
	var columns = make(map[string]interface{})
	for _, row := range dataset.Rows {
		for k := range row.Values {
			columns[k] = true
		}
	}
	return toolbox.MapKeysToStringSlice(columns)

}

func updateDatasetDescriptorIfNeeded(manager dsc.Manager, dataset *Dataset) {
	if len(dataset.PkColumns) == 0 {
		dataset.Columns = buildColumnsForDataset(dataset)
		tableDescriptor := manager.TableDescriptorRegistry().Get(dataset.Table)
		dataset.Autoincrement = tableDescriptor.Autoincrement
		dataset.PkColumns = tableDescriptor.PkColumns
		dataset.FromQuery = tableDescriptor.FromQuery
	}
}

//ExpectDatasets verifies that passed in expected dataset data values are present in the datastore, this methods reports any violations.
func (tm *datasetTestManager) ExpectDatasets(checkPolicy int, datasets *Datasets) (AssertViolations, error) {
	context := toolbox.NewContext()
	manager := tm.managerRegistry.Get(datasets.Datastore)
	var result = make([]AssertViolation, 0)

	for i := range datasets.Datasets {
		err := tm.expandTable(datasets.Datasets[i])
		if err != nil {
			return nil, err
		}
		err = tm.expandFromQuery(context, datasets.Datasets[i])
		if err != nil {
			return nil, fmt.Errorf("Failed to prepare datastore %v - unable to expand macro in the fromQuery %v due to %v", datasets.Datastore, datasets.Datasets[i].Table, err)
		}
		updateDatasetDescriptorIfNeeded(manager, datasets.Datasets[i])
		mapper := newDatasetRowMapper(datasets.Datasets[i].Columns, nil)
		err = tm.expandMacros(context, datasets.Datastore, manager, datasets.Datasets[i])
		if err != nil {
			return nil, err
		}
		switch checkPolicy {
		case FullTableDatasetCheckPolicy:
			voliation, err := tm.expectFullDatasets(manager, datasets.Datastore, datasets.Datasets[i], mapper)
			if err != nil {
				return nil, err
			}
			result = append(result, voliation...)
			continue
		case SnapshotDatasetCheckPolicy:
			voliation, err := tm.expectSnapshotDatasets(manager, datasets.Datastore, datasets.Datasets[i], mapper)
			if err != nil {
				return nil, err
			}
			result = append(result, voliation...)
			continue
		default:
			panic(fmt.Sprintf("Unsupported policy: %v", checkPolicy))
		}
	}

	return NewAssertViolations(result), nil

}

//ManagerRegistry returns ManagerRegistry.
func (tm *datasetTestManager) ManagerRegistry() dsc.ManagerRegistry {
	return tm.managerRegistry
}

//ValueProviderRegistry returns macro value provider registry.
func (tm *datasetTestManager) ValueProviderRegistry() toolbox.ValueProviderRegistry {
	return tm.valueProviderRegistry
}

//DatasetFactory returns dataset factory.
func (tm *datasetTestManager) DatasetFactory() DatasetFactory {
	return tm.datasetFactory
}

//MacroEvaluator returns macro evaluator.
func (tm *datasetTestManager) MacroEvaluator() *toolbox.MacroEvaluator {
	return tm.macroEvaluator
}

//RegisterTable register table descriptor within datastore manager.
func (tm *datasetTestManager) RegisterTable(datastore string, tableDescriptor *dsc.TableDescriptor) {
	manager := tm.managerRegistry.Get(datastore)
	manager.TableDescriptorRegistry().Register(tableDescriptor)
}

//RegisteredTables returns all registered table for passed in datastore.
func (tm *datasetTestManager) RegisteredTables(datastore string) []string {
	manager := tm.managerRegistry.Get(datastore)
	return manager.TableDescriptorRegistry().Tables()
}

//RegisterDatasetMapping registers dataset mapping for passed in name.
//Note that dataset mapping name should never be the actual table name, as this method will create table descriptor for the mapping.
func (tm *datasetTestManager) RegisterDatasetMapping(name string, mapping *DatasetMapping) {
	tm.datasetMappingRegistry.register(name, mapping)
}

//RegisteredMapping returns registered dataset mapping names
func (tm *datasetTestManager) RegisteredMapping() []string {
	return (*tm.datasetMappingRegistry).names()
}

func registerValueProvider(registry toolbox.ValueProviderRegistry) {
	registry.Register("seq", newSequenceValueProvider())
	registry.Register("pos", newPositionValueProvider())
	registry.Register("sql", newQueryValueProvider())
	registry.Register("nil", toolbox.NewNilValueProvider())
	registry.Register("env", toolbox.NewEnvValueProvider())
	registry.Register("cast", toolbox.NewCastedValueProvider())
	registry.Register("current_timestamp", toolbox.NewCurrentTimeProvider())
	registry.Register("current_date", toolbox.NewCurrentDateProvider())
	registry.Register("between", newBetweenPredicateValueProvider())
	registry.Register("within_sec", newWithinSecPredicateValueProvider())
	registry.Register("fromQuery", newBgQueryProvider())
	registry.Register("table_id", newFileValueProvider())
}

//NewDatasetTestManager returns a new DatasetTestManager
func NewDatasetTestManager() DatasetTestManager {
	valueRegistryProvider := toolbox.NewValueProviderRegistry()
	registerValueProvider(valueRegistryProvider)
	macroEvaluator := &toolbox.MacroEvaluator{ValueProviderRegistry: valueRegistryProvider, Prefix: "<ds:", Postfix: ">"}
	var datatestManager = &datasetTestManager{
		managerRegistry:        dsc.NewManagerRegistry(),
		valueProviderRegistry:  valueRegistryProvider,
		macroEvaluator:         macroEvaluator,
		datasetMappingRegistry: newDatasetTransformerRegistry(),
	}
	datatestManager.datasetFactory = newDatasetFactory(datatestManager.managerRegistry)
	return datatestManager
}
