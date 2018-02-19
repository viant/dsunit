package snapshot

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"testing"

	"github.com/stretchr/testify/assert"
	"os"
)

func TestDatastoreDatasetSnapshotTaker_Take(t *testing.T) {

	if os.Getenv("TestDatastoreDatasetSnapshotTaker") != "" {
		dbRegistry := dsc.NewManagerRegistry()
		managerFactory := dsc.NewManagerFactory()
		config := dsc.NewConfig("mysql", "[user]:[password]@[url]", "user:root,password:****,url:tcp(127.0.0.1:3306)/mydb1?parseTime=true")
		dbManager, _ := managerFactory.Create(config)
		dbRegistry.Register("mydb1", dbManager)

		snapshotManager := dsunit.NewDatastoreDatasetSnapshotManager(dbRegistry)
		assert.NotNil(t, snapshotManager)
		err := snapshotManager.Take("mydb1", "/tmp/mydb1")
		assert.Nil(t, err)
	}
}
