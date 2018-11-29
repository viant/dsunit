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
	//casandraSetup(t)
	dsc.Logf = dsc.StdoutLogger
	//defer casandraTearDown(t)
	if dsunit.InitFromURL(t, "config/init.json") {
		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		err := casandraRunSomeBusinessLogic()
		if !assert.Nil(t, err) {
			return
		}
		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
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

var credentials = url.NewResource("config/secret.json").URL

func startCasandra() error {


	/*
	_, err := endlyManager.Run(endlyContext, &docker.RunRequest{
		Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
		Image:  "cassandra:3.0",
		Ports: map[string]string{
			"7000": "7000",
			"7001": "7001",
			"7199": "7199",
			"9042": "9042",
			"9160": "9160",
		},
		Name: "casandra_dsunit",
	})
	if err != nil {
		return err
	}

	*/
	//it takes some time to docker container to fully start
	config, err := dsc.NewConfigWithParameters("cql", "127.0.0.1?keyspace=mydb", "", map[string]interface{}{
		"keyspace":"mydb",
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
		_, err = dscManager.ReadSingle(&record, "SELECT * FROM system.local", nil, nil)
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

func stopCasandra() error {
	_, err := endlyManager.Run(endlyContext, &docker.StopRequest{
		&docker.BaseRequest{
			Target: url.NewResource("ssh://127.0.0.1", localhostCredential),
			Name:   "casandra_dsunit",
		},
	})
	if err != nil {
		return err
	}
	return err

}
