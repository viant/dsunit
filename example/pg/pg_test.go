package example

import (
	"fmt"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/endly"
	"github.com/viant/endly/system/docker"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/secret"
	"github.com/viant/toolbox/url"
	"testing"
)

/*
Prerequisites:
1.docker service running
2. localhost credentials  to conneect to the localhost vi SSH
	or generate ~/.secret/localhost.json with  endly -c=localhost option
*/

//Global variables for all test integrating with endly.
var endlyManager = endly.New()
var endlyContext = endlyManager.NewContext(toolbox.NewContext())

func setup(t *testing.T) {
	err := startPostgres()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func tearDown(t *testing.T) {
	err := stopPostgres()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_Postgres(t *testing.T) {
	setup(t)
	defer tearDown(t)
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

func runSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("postgres", "host=127.0.0.1 port=5432 user=[username] password=[password] dbname=mydb sslmode=disable", pgCredential, nil)
	if err != nil {
		return err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	result, err := manager.Execute("UPDATE users SET comments = ? WHERE username = ?", "dsunit test", "Vudi")
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

var pgCredential = url.NewResource("config/secret.json").URL


func startPostgres() error {
	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "postgres:9.6-alpine",
		Env: map[string]string{
			"POSTGRES_PASSWORD": "**pg**",
			"POSTGRES_USER":     "##pg##",
		},
		Secrets: map[secret.SecretKey]secret.Secret{
			"pg": secret.Secret(pgCredential),
		},
		Name: "pg_dsunit",
		Ports: map[string]string{
			"5432": "5432",
		},
	})
	return err
}

func stopPostgres() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "pg_dsunit",
	})
	if err != nil {
		return err
	}
	return err

}
