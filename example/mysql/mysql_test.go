package mysql

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/dsunit/url"
	"github.com/viant/endly"
	"github.com/viant/endly/system/docker"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/secret"
	"path"
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

func mySQLSetup(t *testing.T) {
	err := startMySQL()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func mySQLTearDown(t *testing.T) {
	err := stopMySQL()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_MySQL(t *testing.T) {
	mySQLSetup(t)
	defer mySQLTearDown(t)

	if dsunit.InitFromURL(t, "config/init.json") {

		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		err := mySQLRunSomeBusinessLogic()
		if !assert.Nil(t, err) {
			return
		}
		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")

		parent := toolbox.CallerDirectory(3)
		service := dsunit.New()
		registerRequest, _ := dsunit.NewRegisterRequestFromURL(path.Join(parent, "config/init.yaml"))
		{
			resp := service.Register(registerRequest)
			assert.Equal(t, "ok", resp.Status)
		}

		dumpRequest, _ := dsunit.NewDumpRequestFromURL(path.Join(parent, "dump_req.yaml"))
		resp := service.Dump(dumpRequest)
		assert.Equal(t, "ok", resp.Status)

	}
}

func mySQLRunSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("mysql", "[username]:[password]@tcp(127.0.0.1:3306)/mydb?parseTime=true", mysqlCredentials, nil)
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

var mysqlCredentials = url.NewResource("config/secret.json").URL

func startMySQL() error {
	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "mysql:5.6",
		Ports: map[string]string{
			"3306": "3306",
		},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "**mysql**",
		},
		Secrets: map[secret.SecretKey]secret.Secret{
			"**mysql**": secret.Secret(mysqlCredentials),
		},

		Mount: map[string]string{
			"/tmp/my.cnf": "/etc/my.cnf",
		},
		Name: "mysql_dsunit",
	})
	return err
}

func stopMySQL() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "mysql_dsunit",
	})
	return err

}
