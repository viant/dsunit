package aerospike

import (
	"fmt"
	_ "github.com/adrianwit/mgc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/endly"
	"github.com/viant/endly/system/docker"
	"github.com/viant/toolbox"
	"testing"
)

/*
Prerequisites:
1.docker service running
*/

//Global variables for all test integrating with endly.
var endlyManager = endly.New()
var endlyContext = endlyManager.NewContext(toolbox.NewContext())

func setup(t *testing.T) {
	err := startMongo()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func tearDown(t *testing.T) {
	err := stopMongo()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_Mongo(t *testing.T) {
	setup(t)
	defer tearDown(t)
	//dsc.Logf = dsc.StdoutLogger

	if dsunit.InitFromURL(t, "config/init.json") {
		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		err := runSomeBusinessLogic()
		if !assert.Nil(t, err) {
			return
		}
		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}

func getConfig() (*dsc.Config, error) {
	return dsc.NewConfigWithParameters("mgc", "", "", map[string]interface{}{
		"dbname":    "mydb",
		"host":      "127.0.0.1",
		"keyColumn": "id",
	})
}

func runSomeBusinessLogic() error {
	config, err := getConfig()
	if err != nil {
		return err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	result, err := manager.Execute("UPDATE users SET comments = ? WHERE id = ?", "dsunit test", 3)
	if err != nil {
		return err
	}
	sqlResult, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if sqlResult != 1 {
		return fmt.Errorf("expected one row updated but had: %v", sqlResult)
	}
	return nil
}

func startMongo() error {

	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "mongo:latest",
		Ports: map[string]string{
			"27017": "27017",
		},
		Name: "mongo_dsunit",
	})
	return err
}

func stopMongo() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "mongo_dsunit",
	})
	if err != nil {
		return err
	}
	return err

}
