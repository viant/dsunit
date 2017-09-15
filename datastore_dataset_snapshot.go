package dsunit

import (
	"github.com/viant/dsc"
	"path"
	"github.com/viant/toolbox"
	"os"
	"fmt"
)

var fileMode os.FileMode = 0644


type DatastoreDatasetSnapshotManager struct {
	registry dsc.ManagerRegistry
}

func (t *DatastoreDatasetSnapshotManager) Take(datastore string, targetDirectory string) error {
	manager :=t.registry.Get(datastore)
	if manager == nil {
		return fmt.Errorf("Failed to lookup datastor manager: %v", datastore)
	}
	provider :=  NewDatastoreDatasetProvider(manager)
	dbConfig := manager.Config()
	dialect := dsc.GetDatastoreDialect(dbConfig.DriverName)
	tables, err := dialect.GetTables(manager, datastore)
	if err != nil {
		return err
	}
	for _, table := range tables {

		dataset, err := provider.Get(table)
		if err != nil {
			return err
		}

		tableDatasetFile := path.Join(targetDirectory, dataset.Table +".json")
		writer, err := os.OpenFile(tableDatasetFile, os.O_CREATE|os.O_WRONLY, fileMode)
		if err != nil {
			return err
		}
		defer writer.Close()
		err = toolbox.NewJSONEncoderFactory().Create(writer).Encode(dataset.AsMapArray())
		if err != nil  {
			return nil
		}
	}
	return nil
}

func NewDatastoreDatasetSnapshotManager(registry dsc.ManagerRegistry) *DatastoreDatasetSnapshotManager{
	return &DatastoreDatasetSnapshotManager{
		registry:registry,
	}
}