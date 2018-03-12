package mysql

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/endly"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"os"
	"path"
	"strings"
	"testing"
	"time"
	"github.com/viant/endly/system/docker"
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
var localhostCredential = path.Join(os.Getenv("HOME"), ".secret/localhost.json")

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
	}
}

func mySQLRunSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("mysql", "[username]:[password]@tcp(127.0.0.1:3306)/mydb?parseTime=true", mysqlCredential, nil)
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

var mysqlCredential = url.NewResource("config/secret.json").URL

func startMySQL() error {

	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
		Image:  "mysql:5.6",
		Ports: map[string]string{
			"3306": "3306",
		},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": "**mysql**",
		},
		Secrets: map[string]string{
			"**mysql**": mysqlCredential,
		},

		Mount: map[string]string{
			"/tmp/my.cnf": "/etc/my.cnf",
		},
		Name: "mysql_dsunit",
	})
	if err != nil {
		return err
	}
	//it takes some time to docker container to fully start
	config, err := dsc.NewConfigWithParameters("mysql", "[username]:[password]@tcp(127.0.0.1:3306)/mysql?parseTime=true", mysqlCredential, nil)
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
		_, err = dscManager.ReadSingle(&record, "SELECT NOW() AS ts", nil, nil)
		if err == nil {
			time.Sleep(2 * time.Second)
			break
		}
		if !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "bad connection") {
			return err
		}
		time.Sleep(5 * time.Second)
	}
	return err
}

func stopMySQL() error {
	_, err := endlyManager.Run(endlyContext, &docker.ContainerStopRequest{
		&docker.ContainerBaseRequest{
			Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
			Name:   "mysql_dsunit",
		},
	})
	if err != nil {
		return err
	}
	return err

}
