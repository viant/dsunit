package example

import (
	"fmt"
	_ "github.com/mattn/go-oci8"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/endly"
	"github.com/viant/endly/system/docker"
	"github.com/viant/toolbox"
	"strings"
	"testing"
)

/*
Prerequisites:
1. docker service running
2. Oracle instance client SDK.(client and sdk)

	https://github.com/mattn/go-oci8

	go get github.com/mattn/go-oci8

*/

//Global variables for all test integrating with endly.
var endlyManager = endly.New()
var endlyContext = endlyManager.NewContext(toolbox.NewContext())

func setup(t *testing.T) {

	err := startOracle()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func tearDown(t *testing.T) {
	err := stopOracle()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_Oracle(t *testing.T) {
	setup(t)
	defer tearDown(t)

	//	showCreateTable()

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

func showCreateTable() error {
	config, err := dsc.NewConfigWithParameters("oci8", "[username]/[password]@127.0.0.1:1521/xe", "ora-e2e", nil)
	if err != nil {
		return err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	dialect := dsc.GetDatastoreDialect("oci8")
	DDL, err := dialect.ShowCreateTable(manager, "events")

	dd := strings.Replace(DDL, "events", "events_993999", 1)
	dd = strings.Replace(dd, ";", "", 1)

	_, err = manager.Execute(dd)
	fmt.Printf("DML: %v %v\n", DDL, err)
	return nil
}

func runSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("oci8", "mydb/oracle@127.0.0.1:1521/xe", "", nil)
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

func startOracle() error {

	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "thebookpeople/oracle-xe-11g",
		Env: map[string]string{
			"ORACLE_ALLOW_REMOTE": "true",
		},
		Name: "ora_dsunit",
		Ports: map[string]string{
			"1521": "1521",
		},
	})
	return err
}

func stopOracle() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "ora_dsunit",
	})
	return err

}
