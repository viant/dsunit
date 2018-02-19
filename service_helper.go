package dsunit

import (
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

//GetDatastoreDialect return GetDatastoreDialect for supplied datastore and registry.
func  GetDatastoreDialect(datastore string, registry dsc.ManagerRegistry) dsc.DatastoreDialect {
	manager := registry.Get(datastore)
	dbConfig := manager.Config()
	return dsc.GetDatastoreDialect(dbConfig.DriverName)
}


//RecreateDatastore recreates target datastore from supplied admin datastore and registry
func  RecreateDatastore(adminDatastore, targetDatastore string, registry dsc.ManagerRegistry) error {
	dialect := GetDatastoreDialect(adminDatastore, registry)
	adminManager := registry.Get(adminDatastore)
	if !dialect.CanDropDatastore(adminManager) {
		return  recreateTables(registry, targetDatastore)
	}
	return recreateDatastore(adminManager, registry, targetDatastore)
}



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



func  recreateDatastore(manager dsc.Manager, registry dsc.ManagerRegistry, datastore string) (err error) {
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