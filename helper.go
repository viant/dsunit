package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/assertly"
	"fmt"
	"strings"
	"runtime"
)


//TestSchema represents a placeholder to be replaced with location where go test file are located
const TestSchema = "test://"


func recreateTables(registry dsc.ManagerRegistry, datastore string) error {
	manager := registry.Get(datastore)
	dialect := GetDatastoreDialect(datastore, registry)
	tables, err := dialect.GetTables(manager, datastore)
	if err != nil {
		return err
	}
	var existingTables = make(map[string]bool)
	toolbox.SliceToMap(tables, existingTables, toolbox.CopyStringValueProvider, toolbox.TrueValueProvider)
	tableRegistry := manager.TableDescriptorRegistry()
	for _, table := range tableRegistry.Tables() {
		if _, found := existingTables[table]; found {
			err := dialect.DropTable(manager, datastore, table)
			if err != nil {
				return err
			}
		}
		descriptor := tableRegistry.Get(table)
		if descriptor.HasSchema() {
			err := dialect.CreateTable(manager, datastore, table, "")
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func recreateDatastore(manager dsc.Manager, registry dsc.ManagerRegistry, datastore string) (err error) {
	dialect := GetDatastoreDialect(datastore, registry)
	if err = dropDatastoreIfNeeded(manager, dialect, datastore); err != nil {
		return err
	}
	return dialect.CreateDatastore(manager, datastore)
}

func dropDatastoreIfNeeded(manager dsc.Manager, dialect dsc.DatastoreDialect, datastore string) (err error) {
	var datastores []string
	if datastores, err = dialect.GetDatastores(manager); err == nil {
		hasDatastore := toolbox.HasSliceAnyElements(datastores, datastore)
		if hasDatastore {
			err = dialect.DropDatastore(manager, datastore)
		}
	}
	return err
}

func directiveScan(records []map[string]interface{}, recordHandler func(record Record)) {
	var count = 2;
	if count > len(records) {
		count = len(records)
	}
	for i := 0; i < count; i++ { //first record could be a valid directive
		recordHandler(Record(records[i]))
	}
}



func getTableDescriptor(dataset *Dataset, manager dsc.Manager, context toolbox.Context) (*dsc.TableDescriptor, error) {
	macroEvaluator := assertly.NewDefaultMacroEvaluator()
	expandedTable, err := macroEvaluator.Expand(context, dataset.Table)
	if err != nil {
		return nil, err
	}
	tableName :=toolbox.AsString(expandedTable)
	table := manager.TableDescriptorRegistry().Get(tableName)
	if table == nil {
		table = &dsc.TableDescriptor{Table:tableName}
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
				dataset.Records[0] = make(map[string]interface{})
			}
			dataset.Records[0][assertly.IndexByDirective] = table.PkColumns
		}
	}
	var columns = dataset.Records.Columns()
	if len(columns) > 0 {
		table.Columns = columns
	}
	return table, nil
}


func insertSQLProvider(provider *datasetDmlProvider)  func(item interface{}) *dsc.ParametrizedSQL {
	return func(item interface{}) *dsc.ParametrizedSQL {
		return provider.Get(dsc.SQLTypeInsert, item)
	}
}

//validateDatastores check if registry has all supplied datastore
func validateDatastores(registery dsc.ManagerRegistry, response *BaseResponse, datastores ...string) bool {
	for _, datastore := range datastores {
		if registery.Get(datastore) == nil {
			response.SetErrror(fmt.Errorf("unknown datastore: %v", datastore))
			return false
		}
	}
	return true
}


func expandDscConfig(config *dsc.Config, datastore string)  *dsc.Config {
	config.Parameters["name"] = datastore
	config.Init()
	return config
}


func buildBatchedPkValues(records Records, pkColumns []string) [][]interface{} {
	var result= make([][]interface{}, 0)
	for _, record := range records {
		var pkRecord= make([]interface{}, 0)
		for _, pkColumn := range pkColumns  {
			pkRecord = append(pkRecord, record[pkColumn])
		}
		result = append(result, pkRecord)
	}
	return result
}




func getCallerInfo(callerIndex int) (string, string, int) {
	var callerPointer = make([]uintptr, 10) // at least 1 entry needed
	runtime.Callers(callerIndex, callerPointer)
	callerInfo := runtime.FuncForPC(callerPointer[0])
	file, line := callerInfo.FileLine(callerPointer[0])
	callerName := callerInfo.Name()
	dotPosition := strings.LastIndex(callerName, ".")
	return file, callerName[dotPosition+1:], line
}