package dsunit_test

import (
	"testing"
	"github.com/viant/dsunit"
	"github.com/viant/dsc"
	_ "github.com/go-sql-driver/mysql"

	"github.com/stretchr/testify/assert"
)

func TestDatastoreDatasetSnapshotTaker_Take(t *testing.T) {

	dbRegistry := dsc.NewManagerRegistry()
	managerFactory := dsc.NewManagerFactory()
	config := dsc.NewConfig("mysql", "[user]:[password]@[url]", "user:root,password:dev,url:tcp(127.0.0.1:3306)/test?parseTime=true")
	dbManager, _ := managerFactory.Create(config)
	dbRegistry.Register("test", dbManager)

	snapshotManager := dsunit.NewDatastoreDatasetSnapshotManager(dbRegistry)
	assert.NotNil(t, snapshotManager)
	//err :=snapshotManager.Take("test", dsunit.ExpandTestProtocolAsPathIfNeeded("test://test/snapshot"))
	//assert.Nil(t, err)
}
