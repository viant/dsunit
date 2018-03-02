package aerospike

import (
	"fmt"
	_ "github.com/adrianwit/mgc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/endly"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"os"
	"path"
	"testing"
	"time"
)

/*
Prerequisites:
1.docker service running
2. localhost credentials  to conneect to the localhost vi SSH
	or generate ~/.secret/localhost.json with  endly -c=localhost option
*/

//Global variables for all test integrating with endly.
var endlyManager = endly.NewManager()
var endlyContext = endlyManager.NewContext(toolbox.NewContext())
var localhostCredential = path.Join(os.Getenv("HOME"), ".secret/localhost.json")

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
		"dbname":        "mydb",
		"host":          "127.0.0.1",
		"keyColumnName": "id",
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

	_, err := endlyManager.Run(endlyContext, &endly.DockerRunRequest{
		Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
		Image:  "mongo:latest",
		MappedPort: map[string]string{
			"27017": "27017",
		},
		Name: "mongo_dsunit",
	})
	if err != nil {
		return err
	}
	//it takes some time to docker container to fully start

	config, err := getConfig()
	if err != nil {
		return err
	}

	dscManager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	defer dscManager.ConnectionProvider().Close()
	//wait for docker to fully start
	time.Sleep(5 * time.Second)
	return err
}

func stopMongo() error {
	_, err := endlyManager.Run(endlyContext, &endly.DockerContainerStopRequest{
		&endly.DockerContainerBaseRequest{
			Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
			Name:   "mongo_dsunit",
		},
	})
	if err != nil {
		return err
	}
	return err

}
