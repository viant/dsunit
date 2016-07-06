package dsunit_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/dsunit"
	"time"
)

func init() {
	//dsunit.UseRemoteTestServer("http://localhost:8528")
}


func TestSetup(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
	dsunit.PrepareDatastore(t, "bar_test")
	//business test logic comes here
	dsunit.ExpectDatasets(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy)
}

func TestTest1p(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	//business test logic comes here
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")
}

func TestDatasetMapping(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
}



func TestRemoteSetup(t *testing.T) {
	go func() {
		dsunit.StartServer("8528")
	}()
	time.Sleep(1 * time.Second)

	dsunit.UseRemoteTestServer("http://localhost:8528")


	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	//business test logic comes here
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")


	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
}
