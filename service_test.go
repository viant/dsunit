package dsunit_test

import (
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
	"time"
)

func init() {
	go func() {
		dsunit.StartServer("8528")
	}()
	//dsunit.UseRemoteTestServer("http://localhost:8528")
}

var macroDatastore string = "<ds:env[\"GOOGLE_SERVICE_DATASET_ID\"]>"

func TestSetup(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")
	dsunit.PrepareDatastore(t, "bar_test")
	//business test logic comes here
	dsunit.ExpectDatasets(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy)
}

func TestTest1p(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	dsunit.PrepareDatastoreFor(t, macroDatastore, "test://test/", "test1")
	//business test logic comes here
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")
	dsunit.ExpectDatasetFor(t, macroDatastore, dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")
}

func TestDatasetMapping(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
}

func TestExpectDatasetsFromURL(t *testing.T) {

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	dsunit.PrepareDatastoreFor(t, macroDatastore, "test://test/", "test1")

	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/service_expect.json")
		response := dsunit.GetService().ExpectDatasetsFromURL(url)
		assert.Equal(t, "ok", response.Status)
	}
	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/noexisting.json")
		response := dsunit.GetService().ExpectDatasetsFromURL(url)
		assert.Equal(t, "error", response.Status)
	}

	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/service_expect_failure.json")
		response := dsunit.GetService().ExpectDatasetsFromURL(url)
		assert.Equal(t, "error", response.Status)
	}

}

func TestPrepareDatastoreFromURL(t *testing.T) {
	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/service_prepare.json")
		response := dsunit.GetService().PrepareDatastoreFromURL(url)
		assert.Equal(t, "ok", response.Status)
	}
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, macroDatastore, dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")

	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/nonexisting.json")
		response := dsunit.GetService().PrepareDatastoreFromURL(url)
		assert.Equal(t, "error", response.Status)
	}

}

func TestRemoteSetup(t *testing.T) {

	time.Sleep(1 * time.Second)
	dsunit.UseRemoteTestServer("http://localhost:8528")

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")

	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "test1")
	//business test logic comes here
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "test1")

	dsunit.InitDatastoreFromURL(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromURL(t, "test://test/vertica_script_request.json")
	dsunit.PrepareDatastoreFor(t, "bar_test", "test://test/", "mapping")
	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy, "test://test/", "mapping")
}
