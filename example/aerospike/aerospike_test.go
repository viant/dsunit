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
	"github.com/viant/toolbox/url"
	"os"
	"path"
	"strings"
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
var endlyManager = endly.New()
var endlyContext = endlyManager.NewContext(toolbox.NewContext())
var localhostCredential = path.Join(os.Getenv("HOME"), ".secret/localhost.json")

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
		Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
		Image:  "aerospike/aerospike-server:latest",
		Ports: map[string]string{
			"3000": "3000",
			"3001": "3001",
			"3002": "3002",
			"3004": "3004",
			"8081": "8081",
		},
		Name: "aerospike_dsunit",
	})
	if err != nil {
		return err
	}
	//it takes some time to docker container to fully start

	config, err := dsc.NewConfigWithParameters("aerospike", "tcp(127.0.0.1:3000)/[namespace]", "", map[string]interface{}{
		"namespace": "test",
		"host":      "127.0.0.1",
	})
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
		_, err = dscManager.ReadSingle(&record, "SELECT id FROM users", nil, nil)
		if err == nil {
			time.Sleep(2 * time.Second)
			break
		}
		if !strings.Contains(err.Error(), "Suffix not available") {
			return err
		}
		time.Sleep(2 * time.Second)
	}
	return err
}

func stopAerospike() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		&docker.BaseRequest{
			Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
			Name:   "aerospike_dsunit",
		},
	})
	if err != nil {
		return err
	}
	return err

}
