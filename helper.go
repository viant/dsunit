package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"fmt"
	"strings"
	"runtime"
	"path"
)



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



func insertSQLProvider(provider *datasetDmlProvider)  func(item interface{}) *dsc.ParametrizedSQL {
	return func(item interface{}) *dsc.ParametrizedSQL {
		return provider.Get(dsc.SQLTypeInsert, item)
	}
}

//validateDatastores check if registry has all supplied datastore
func validateDatastores(registery dsc.ManagerRegistry, response *BaseResponse, datastores ...string) bool {
	for _, datastore := range datastores {
		if registery.Get(datastore) == nil {
			response.SetError(fmt.Errorf("unknown datastore: %v", datastore))
			return false
		}
	}
	return true
}


func expandDscConfig(config *dsc.Config, datastore string)  (*dsc.Config, error) {
	if len(config.Parameters) == 0 {
		config.Parameters = make(map[string]string)
	}
	config.Parameters["dbname"] = datastore
	err := config.Init()
	return config, err
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


func hasMatch(target string, candidates ... string) bool {
	for _, candidate := range candidates {
		if strings.HasSuffix(target, candidate) {
			return true
		}
	}
	return false
}

func discoverCaller(offset, maxDepth int, ignoreFiles ...string) (string, string, int) {
	var callerPointer = make([]uintptr, maxDepth) // at least 1 entry needed
	var caller *runtime.Func
	var filename string
	var line int
	for  i := offset;i<maxDepth;i++ {
		runtime.Callers(i, callerPointer)
		caller = runtime.FuncForPC(callerPointer[0])
		filename, line = caller.FileLine(callerPointer[0])
		if hasMatch(filename, ignoreFiles...) {
			continue
		}
		break
	}
	callerName := caller.Name()
	dotPosition := strings.LastIndex(callerName, ".")
	return filename, callerName[dotPosition+1:], line
}



func convertToLowerUnderscore(upperCamelCase string) string {
	if len(upperCamelCase) == 0 {
		return ""
	}
	upperCount := 0;
	result := strings.ToLower(upperCamelCase[0:1])
	for i := 1; i < len(upperCamelCase); i++ {
		aChar := upperCamelCase[i : i+1]

		isUpperCase := strings.ToUpper(aChar) == aChar
		if isUpperCase {
			upperCount++
		} else {
			upperCount = 0
		}

		if isUpperCase && !(aChar >= "0" && aChar <= "9")  && aChar != "_" && upperCount == 1 {
			result = result + "_" + strings.ToLower(aChar)
		} else {
			result = result + strings.ToLower(aChar)
		}
	}
	return result
}


func discoverBaseURLAndPrefix(operation string) (string, string) {
	testfile, method, _ := discoverCaller(2,10,  "tester.go", "helper.go","static.go")
	parent, name := path.Split(testfile)
	name = string(name[:len(name)-3]) //remove .go
	var lastSegment = strings.LastIndex(method, "_")
	if lastSegment > 0 {
		method = string(method[lastSegment+1:])
	}
	method = convertToLowerUnderscore(method)
	return parent, fmt.Sprintf(name + "_%v_%v_", method, operation)
}
