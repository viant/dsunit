package example

import (
	"github.com/viant/endly"
	_ "github.com/lib/pq"
	"github.com/viant/toolbox/url"
	"testing"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"fmt"
	"github.com/stretchr/testify/assert"
	"time"
	"strings"
	"github.com/viant/toolbox"
	"path"
	"os"
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
		if ! dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		err := runSomeBusinessLogic()
		if ! assert.Nil(t, err) {
			return
		}
		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}

func runSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("postgres", "host=127.0.0.1 port=5432 user=[username] password=[password] dbname=mydb sslmode=disable", pgCredential, nil);
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

	_, err := endlyManager.Run(endlyContext, &endly.DockerRunRequest{
		Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
		Image:  "postgres:9.6-alpine",
		Env: map[string]string{
			"POSTGRES_PASSWORD": "***pg***",
			"POSTGRES_USER":     "###pg###",
		},
		Credentials: map[string]string{
			"###pg###": pgCredential,
			"***pg***": pgCredential,
		},
		Name: "pg_dsunit",
		MappedPort: map[string]string{
			"5432": "5432",
		},
	})
	if err != nil {
		return err
	}
	//it takes some time to docker container to fully start

	config, err := dsc.NewConfigWithParameters("postgres", "host=127.0.0.1 port=5432 user=[username] password=[password] dbname=postgres sslmode=disable", pgCredential, nil);
	if err != nil {
		return err
	}

	dscManager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	defer dscManager.ConnectionProvider().Close()
	for i := 0; i < 60; i++ {
		var record = make(map[string]interface{})
		_, err = dscManager.ReadSingle(&record, "SELECT current_database() AS name", nil, nil)
		if err == nil {
			time.Sleep(2 * time.Second)
			break
		}
		if ! strings.Contains(err.Error(), "EOF") {
			return err
		}
		time.Sleep(2 * time.Second)
	}
	return err
}

func stopPostgres() error {
	_, err := endlyManager.Run(endlyContext, &endly.DockerContainerStopRequest{
		&endly.DockerContainerBaseRequest{
			Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
			Name:   "pg_dsunit",
		},
	})
	if err != nil {
		return err
	}
	return err

}
