package example

import (
	"fmt"
	_ "github.com/alexbrainman/odbc"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	url "github.com/viant/dsunit/url"
	"github.com/viant/endly"
	"github.com/viant/endly/system/docker"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/secret"
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
	err := startDB()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func tearDown(t *testing.T) {
	err := stopDB()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_DB(t *testing.T) {
	//	setup(t)
	//	defer tearDown(t)
	//dsc.Logf = dsc.StdoutLogger

	if dsunit.InitFromURL(t, "config/init.yaml") {
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

func runSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("odbc", "driver=Vertica;Database=mydb;ServerName=127.0.0.1;port=5433;user=[username];password=[password]", verticaCred, map[string]interface{}{
		"SEARCH_PATH": "mydb",
	})
	if err != nil {
		return err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	result, err := manager.Execute("UPDATE musers SET comments = ? WHERE username = ?", "dsunit test", "Vudi")
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

var verticaCred = url.NewResource("config/secret.json").URL

func startDB() error {
	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "jbfavre/vertica:9.2.0-7_debian-8",
		Env: map[string]string{
			"DATABASE_NAME":     " mydb",
			"DATABASE_PASSWORD": "${vertica.password}",
		},
		Secrets: map[secret.SecretKey]secret.Secret{
			"vertica": secret.Secret(verticaCred),
		},
		Name: "vertica_dsunit",
		Ports: map[string]string{
			"5433": "5433",
		},
	})
	return err
}

func stopDB() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "vertica_dsunit",
	})
	if err != nil {
		return err
	}
	return err

}
