package aerospike

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/asc"
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
	err := startAerospike()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func tearDown(t *testing.T) {
	err := stopAerospike()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
}

func TestDsunit_Aerospike(t *testing.T) {
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
	config, err := dsc.NewConfigWithParameters("aerospike", "tcp(127.0.0.1:3000)/[namespace]", "", map[string]interface{}{
		"namespace": "test",
		"host":      "127.0.0.1",
	})
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

func startAerospike() error {

	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Image: "aerospike/aerospike-server:latest",
		Ports: map[string]string{
			"3000": "3000",
			"3001": "3001",
			"3002": "3002",
			"3004": "3004",
			"8081": "8081",
		},
		Name: "aerospike_dsunit",
	})
	return err
}

func stopAerospike() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		Name: "aerospike_dsunit",
	})
	if err != nil {
		return err
	}
	return err

}
