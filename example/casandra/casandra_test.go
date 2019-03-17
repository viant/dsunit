package mysql

import (
	"fmt"
	_ "github.com/MichaelS11/go-cql-driver"
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

func casandraSetup(t *testing.T) {
	err := startCasandra()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func casandraTearDown(t *testing.T) {
	err := stopCasandra()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_Casandra(t *testing.T) {
	casandraSetup(t)
	defer casandraTearDown(t)
	if !dsunit.InitFromURL(t, "config/init.yaml") {
		return
	}
	if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
		return
	}
	err := casandraRunSomeBusinessLogic()
	if !assert.Nil(t, err) {
		return
	}
	dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
}



func casandraRunSomeBusinessLogic() error {
	config, err := dsc.NewConfigWithParameters("cql", "127.0.0.1?keyspace=mydb", "", nil)
	if err != nil {
		return err
	}
	manager, err := dsc.NewManagerFactory().Create(config)
	if err != nil {
		return err
	}
	result, err := manager.Execute("UPDATE users SET comments = ? WHERE id = ?", "dsunit test", 1)
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




func startCasandra() error {

	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "cassandra:2.1",
		Ports: map[string]string{
			"7000": "7000",
			"7001": "7001",
			"7199": "7199",
			"9042": "9042",
			"9160": "9160",
		},
		Name: "casandra_dsunit",
	})
	return err
}

func stopCasandra() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "casandra_dsunit",
	})
	if err != nil {
		return err
	}
	return err

}

