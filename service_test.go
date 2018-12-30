package dsunit_test

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/viant/assertly"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
	"log"
	"path"
	"testing"
)

func getTestService(dbname string, baseDirectory string, SQLScripts ...string) (dsunit.Service, error) {
	service := dsunit.New()
	filename := path.Join(baseDirectory, fmt.Sprintf("%v.db", dbname))
	toolbox.RemoveFileIfExist(filename)
	{
		response := service.Register(dsunit.NewRegisterRequest(dbname,
			&dsc.Config{
				DriverName: "sqlite3",
				Descriptor: "[url]",
				Parameters: map[string]interface{}{
					"url": filename,
				},
			}))
		if response.Status != dsunit.StatusOk {
			return nil, errors.New(response.Message)
		}
	}
	{
		response := service.Recreate(dsunit.NewRecreateRequest(dbname, dbname))
		if response.Status != dsunit.StatusOk {
			return nil, errors.New(response.Message)
		}
	}
	for _, SQLScript := range SQLScripts {
		response := service.RunScript(dsunit.NewRunScriptRequest(dbname, url.NewResource(SQLScript)))
		if response.Status != dsunit.StatusOk {
			return nil, errors.New(response.Message)
		}
	}
	return service, nil
}

func TestService_Register(t *testing.T) {
	service, err := getTestService("db1", "test/db1/")
	if assert.Nil(t, err) {
		manager := service.Registry().Get("db1")
		assert.NotNil(t, manager)
	}
}

func TestService_RunScript(t *testing.T) {
	_, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
		return
	}
}

func TestService_Prepare(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
		return
	}
	response := service.Prepare(&dsunit.PrepareRequest{
		DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data", "test1_prepare_", ""),
	})

	if assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
		assert.EqualValues(t, "users", response.Modification["users"].Subject)
		assert.EqualValues(t, 4, response.Modification["users"].Added)
		assert.EqualValues(t, 0, response.Modification["users"].Modified)
		assert.EqualValues(t, 0, response.Modification["users"].Deleted)
	}
}

func TestService_Expect(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
		return
	}
	{
		response := service.Prepare(&dsunit.PrepareRequest{
			DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data", "db1_prepare_", ""),
		})
		if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
			return
		}
	}
	response := service.Expect(&dsunit.ExpectRequest{
		DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data", "db1_expect_", ""),
	})

	if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
		return
	}
	assert.EqualValues(t, 18, response.PassedCount)
	assert.EqualValues(t, 0, response.FailedCount)

}

func TestService_Query(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if assert.Nil(t, err) {
		response := service.Prepare(&dsunit.PrepareRequest{
			DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data/", "db1_prepare_", ""),
		})
		if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
			return
		}
		serviceResponse := service.Query(dsunit.NewQueryRequest("db1", "SELECT COUNT(1) AS cnt FROM users"))
		if assert.Equal(t, dsunit.StatusOk, serviceResponse.Status) {
			assert.EqualValues(t, map[string]interface{}{
				"cnt": int64(4),
			}, serviceResponse.Records[0])
		}
	}

}

func TestService_FromQueryValidation(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if assert.Nil(t, err) {
		{
			response := service.Prepare(&dsunit.PrepareRequest{
				DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data/", "db1_prepare_", ""),
			})
			if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
				return
			}
		}

		response := service.Expect(&dsunit.ExpectRequest{
			DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data", "db1_query_expect_", ""),
		})
		if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
			return
		}
		assert.EqualValues(t, 12, response.PassedCount)
		assert.EqualValues(t, 0, response.FailedCount, response.Message)

	}
}

func TestService_GetSequences(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if assert.Nil(t, err) {
		response := service.Prepare(&dsunit.PrepareRequest{
			DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data/", "db1_prepare_", ""),
		})
		if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
			return
		}
		serviceResponse := service.Sequence(dsunit.NewSequenceRequest("db1", "users"))
		if assert.Equal(t, dsunit.StatusOk, serviceResponse.Status) {
			assert.EqualValues(t, 5, serviceResponse.Sequences["users"])
		}
	}
}

func TestService_FreezeDataset(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")

	response := service.Prepare(&dsunit.PrepareRequest{
		DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data/", "db1_prepare_", ""),
	})
	if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
		return
	}

	if assert.Nil(t, err) {

		response := service.Freeze(&dsunit.FreezeRequest{
			Datastore: "db1",
			DestURL:   "/tmp/dsunit/users.json",
			SQL:       "SELECT * FROM users",
		})
		if !assert.EqualValues(t, dsunit.StatusOk, response.Status, response.Message) {
			return
		}
		assert.EqualValues(t, 4, response.Count)
	}
}

func TestService_Compare(t *testing.T) {
	service, err := getTestService("db1", "test/db1/", "test/db1/schema.ddl")
	if !assert.Nil(t, err, fmt.Sprintf("%v", err)) {
		return
	}
	service.Prepare(&dsunit.PrepareRequest{
		DatasetResource: dsunit.NewDatasetResource("db1", "test/db1/data", "test1_prepare_", ""),
	})

	{
		response := service.Compare(&dsunit.CompareRequest{
			Source1: &dsunit.DatastoreSQL{
				Datastore: "db1",
				SQL:       "SELECT * FROM users ORDER BY 1",
			},
			Source2: &dsunit.DatastoreSQL{
				Datastore: "db1",
				SQL:       "SELECT * FROM users ORDER BY 1",
			},
		})
		assert.EqualValues(t, "ok", response.Status)
		assert.EqualValues(t, 0, response.FailedCount)
		assert.EqualValues(t, 6, response.PassedCount)
		assert.EqualValues(t, 1, response.MatchedRows)
	}
	{
		response := service.Compare(&dsunit.CompareRequest{
			Source1: &dsunit.DatastoreSQL{
				Datastore: "db1",
				SQL:       "SELECT * FROM users",
			},
			Source2: &dsunit.DatastoreSQL{
				Datastore: "db1",
				SQL:       "SELECT * FROM users",
			},
			Directives: map[string]interface{}{
				assertly.IndexByDirective: "id",
			},
		})

		if !assert.EqualValues(t, "ok", response.Status) {
			log.Print(response.Message)
			return
		}

		assert.EqualValues(t, 0, response.FailedCount)
		assert.EqualValues(t, 6, response.PassedCount)
		assert.EqualValues(t, 1, response.MatchedRows)
	}

}
