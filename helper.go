package dsunit

import (
	"bytes"
	"fmt"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/data"
	"github.com/viant/toolbox/data/udf"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/url"
	"path"
	"strings"
)

func getDatastoreTables(registry dsc.ManagerRegistry, datastore string) ([]string, error) {
	manager := registry.Get(datastore)
	dialect := GetDatastoreDialect(datastore, registry)
	if hasDatastore(manager, dialect, datastore) {
		return dialect.GetTables(manager, datastore)
	}
	return []string{}, nil
}

func getRegistryTables(registry dsc.ManagerRegistry, datastore string) []string {
	manager := registry.Get(datastore)
	tableRegistry := manager.TableDescriptorRegistry()
	if len(tableRegistry.Tables()) > 0 {
		return tableRegistry.Tables()
	}
	return []string{}
}

func indexTables(tables []string) map[string]bool {
	var index = make(map[string]bool)
	for _, table := range tables {
		index[table] = true
	}
	return index
}

func dropTables(registry dsc.ManagerRegistry, datastore string, tables []string) error {
	manager := registry.Get(datastore)
	dialect := GetDatastoreDialect(datastore, registry)
	for _, table := range tables {
		if err := dialect.DropTable(manager, datastore, table); err != nil {
			return err
		}
	}
	return nil
}

//recreateTables recreate registry or all datastorre table, or just create new if drop flag is false
func recreateTables(registry dsc.ManagerRegistry, datastore string, drop bool) error {
	manager := registry.Get(datastore)
	dialect := GetDatastoreDialect(datastore, registry)
	registryTables := getRegistryTables(registry, datastore)
	dbTables, err := getDatastoreTables(registry, datastore)
	if err != nil {
		return err
	}
	if drop {
		if len(registryTables) == 0 { //drop all database tables
			if err = dropTables(registry, datastore, dbTables); err != nil {
				return err
			}
		} //drop only registry specified tables
		registryTablesExisting := make([]string, 0)
		if err = toolbox.Intersect(registryTables, dbTables, &registryTablesExisting); err != nil {
			return err
		}
		if len(registryTablesExisting) > 0 {
			fmt.Printf("Dropping from %s following tables %v \n", datastore, registryTablesExisting)
		} else {
			fmt.Println("No tables to drop in datastore " + datastore)
		}
		if err = dropTables(registry, datastore, registryTablesExisting); err != nil {
			return err
		}
	}
	//no registry table - quit
	if len(registryTables) == 0 {
		return nil
	}
	dbTables, err = getDatastoreTables(registry, datastore)
	if err != nil {
		return err
	}
	existingTable := indexTables(dbTables)
	tableRegistry := manager.TableDescriptorRegistry()
	for _, table := range registryTables {
		descriptor := tableRegistry.Get(table)
		if !descriptor.HasSchema() {
			continue
		}
		if _, hasTable := existingTable[table]; hasTable {
			continue
		}
		err := dialect.CreateTable(manager, datastore, table, "")
		if err != nil {
			return err
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

func hasDatastore(manager dsc.Manager, dialect dsc.DatastoreDialect, datastore string) bool {
	if datastores, err := dialect.GetDatastores(manager); err == nil {
		for _, candidate := range datastores {
			if candidate == datastore {
				return true
			}
		}
	}
	return false
}

func dropDatastoreIfNeeded(manager dsc.Manager, dialect dsc.DatastoreDialect, datastore string) (err error) {
	if !hasDatastore(manager, dialect, datastore) {
		return
	}
	return dialect.DropDatastore(manager, datastore)
}

func directiveScan(records []map[string]interface{}, recordHandler func(record Record)) {
	var count = 2
	if count > len(records) {
		count = len(records)
	}
	for i := 0; i < count; i++ { //first record could be a valid directive
		recordHandler(Record(records[i]))
	}
}

func insertSQLProvider(provider *datasetDmlProvider) func(item interface{}) *dsc.ParametrizedSQL {
	return func(item interface{}) *dsc.ParametrizedSQL {
		return provider.Get(dsc.SQLTypeInsert, item)
	}
}

//validateDatastores check if registry has all supplied datastore
func validateDatastores(registry dsc.ManagerRegistry, response *BaseResponse, datastores ...string) bool {
	for _, datastore := range datastores {
		if registry.Get(datastore) == nil {
			response.SetError(fmt.Errorf("unknown datastore: %v", datastore))
			return false
		}
	}
	return true
}

func expandDscConfig(config *dsc.Config, datastore string) (*dsc.Config, error) {
	if len(config.Parameters) == 0 {
		config.Parameters = make(map[string]interface{})
	}
	config.Parameters["dbname"] = datastore
	err := config.Init()
	return config, err
}

func buildBatchedPkValues(records Records, pkColumns []string) [][]interface{} {
	var result = make([][]interface{}, 0)
	for _, record := range records {
		var pkRecord = make([]interface{}, 0)
		for _, pkColumn := range pkColumns {
			pkRecord = append(pkRecord, record[pkColumn])
		}
		result = append(result, pkRecord)
	}
	return result
}

func convertToLowerUnderscore(upperCamelCase string) string {
	if len(upperCamelCase) == 0 {
		return ""
	}
	upperCount := 0
	result := strings.ToLower(upperCamelCase[0:1])
	for i := 1; i < len(upperCamelCase); i++ {
		aChar := upperCamelCase[i : i+1]

		isUpperCase := strings.ToUpper(aChar) == aChar
		if isUpperCase {
			upperCount++
		} else {
			upperCount = 0
		}

		if isUpperCase && !(aChar >= "0" && aChar <= "9") && aChar != "_" && upperCount == 1 {
			result = result + "_" + strings.ToLower(aChar)
		} else {
			result = result + strings.ToLower(aChar)
		}
	}
	return result
}

func discoverBaseURLAndPrefix(operation string) (string, string) {
	testfile, method, _ := toolbox.DiscoverCaller(2, 10, "tester.go", "helper.go", "static.go")
	parent, name := path.Split(testfile)
	name = string(name[:len(name)-3]) //remove .go
	var lastSegment = strings.LastIndex(method, "_")
	if lastSegment > 0 {
		method = string(method[lastSegment+1:])
	}
	method = convertToLowerUnderscore(method)
	return parent, fmt.Sprintf(name+"_%v_%v_", method, operation)
}

func escapeVariableIfNeeded(val string) string {
	if strings.HasPrefix(val, "$$") { //escaping
		val = strings.Replace(val, "$$", "$", 1)
	}
	return val
}

func expandDataIfNeeded(context toolbox.Context, records []map[string]interface{}) {
	if context.Contains(SubstitutionMapKey) {
		var substitutionMap *data.Map
		if context.GetInto(SubstitutionMapKey, &substitutionMap) {
			for i, record := range records {
				records[i] = toolbox.AsMap(substitutionMap.Expand(record))
			}
		}
		return
	}
	aMap := data.NewMap()
	udf.Register(aMap)
	for i, record := range records {
		records[i] = toolbox.AsMap(aMap.Expand(record))
	}
}

func uploadContent(resource *url.Resource, response *BaseResponse, payload []byte) {
	storageService, err := storage.NewServiceForURL(resource.URL, resource.Credentials)
	if err != nil {
		response.SetError(err)
		return
	}
	err = storageService.Upload(resource.URL, bytes.NewReader(payload))
	response.SetError(err)
}

func removeDirectiveRecord(records []interface{}) []interface{} {
	if len(records) == 0 {
		return records
	}
	theFirst := records[0]

	if toolbox.IsMap(theFirst) {
		aMap := toolbox.AsMap(theFirst)
		for k := range aMap {
			if !strings.HasPrefix(k, "@"+k) {
				return records
			}
		}
	}
	return records[1:]
}
