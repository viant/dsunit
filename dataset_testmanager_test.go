package dsunit_test

import (
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	_ "github.com/viant/bgc"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
)

//var macroDatasetId = "<ds:env[\"GOOGLE_SERVICE_DATASET_ID\"]>"
//var gbqDatasetId string

func Init(t *testing.T) dsunit.DatasetTestManager {
	datasetTestManager := dsunit.NewDatasetTestManager()
	managerRegistry := datasetTestManager.ManagerRegistry()
	managerFactory := dsc.NewManagerFactory()
	var err error
	//gbqServiceAccountId, err := toolbox.ExpandValue(datasetTestManager.MacroEvaluator(), "<ds:env[\"GOOGLE_SERVICE_ACCOUNT_ID\"]>")
	//if err != nil {
	//	t.Fatalf("failed to Init %v", err)
	//}
	//expandedDatasetId, err := toolbox.ExpandValue(datasetTestManager.MacroEvaluator(), macroDatasetId)
	//if err != nil {
	//	t.Fatalf("failed to Init %v", err)
	//}
	//gbqDatasetId = expandedDatasetId

	{
		//admin connection

		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/master.db")
		manager, _ := managerFactory.Create(config)
		managerRegistry.Register("mysql", manager)
	}
	{

		//test connection
		config := dsc.NewConfig("sqlite3", "[url]", "url:./test/test.db")
		manager, _ := managerFactory.Create(config)
		managerRegistry.Register("bar_test", manager)
	}
	//{
	//
	//	//test connection
	//	config := dsc.NewConfig("bigquery", "", "serviceAccountId:" + gbqServiceAccountId + ",privateKeyPath:/etc/test_service.pem,projectId:formal-cascade-571,datasetId:" + gbqDatasetId + ",dateFormat:yyyy-MM-dd hh:mm:ss z,maxResults:500")
	//	manager, _ := managerFactory.Create(config)
	//	schema := []map[string]interface{} {
	//		{
	//			"name": "event_id",
	//			"type": "integer",
	//		},
	//		{
	//			"name": "event_name",
	//			"type": "string",
	//		},
	//		{
	//			"name": "logging_create_time",
	//			"type": "timestamp",
	//			"DefaultValue": "<ds:current_timestamp>",
	//		},
	//	}
	//	fromQuery := "SELECT row_number() over (order by logging_create_time) as position, event_id, event_name, logging_create_time as loggingCreateTime FROM log_history"
	//	manager.TableDescriptorRegistry().Register(&dsc.TableDescriptor{Table: "log_history", PkColumns: []string{"event_id","event_name"}, Columns: []string{"event_id","event_name"}, Schema: schema, FromQuery: fromQuery})
	//	managerRegistry.Register(gbqDatasetId, manager)
	//}
	err = datasetTestManager.ClearDatastore("mysql", "bar_test")
	if err != nil {
		t.Fatalf("failed to RecreateDatastore %v", err)
	}

	//err = datasetTestManager.ClearDatastore(gbqDatasetId, gbqDatasetId)
	//if err != nil {
	//	t.Fatalf("failed to RecreateDatastore %v", err)
	//}

	_, err = datasetTestManager.Execute(&dsunit.Script{
		Datastore: "bar_test",
		Sqls: []string{
			"DROP TABLE IF EXISTS users",
			"CREATE TABLE `users` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`username` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
			"INSERT INTO users(username, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments',CURRENT_TIMESTAMP);",
		},
	})

	if err != nil {
		t.Fatalf("failed to init databsae %v", err)
	}
	datasetTestManager.RegisterTable("bar_test", &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}})

	//fromQuery := "SELECT row_number() over (order by logging_create_time) as position, event_id, event_name, logging_create_time as loggingCreateTime FROM log_history"
	//datasetTestManager.RegisterTable(gbqDatasetId, &dsc.TableDescriptor{Table: "log_history", PkColumns: []string{"event_id","event_name"}, Columns: []string{"event_id","event_name"}, FromQuery: fromQuery})

	return datasetTestManager
}

func TestResetDatastore(t *testing.T) {
	datasetTestManager := Init(t)
	manager := datasetTestManager.ManagerRegistry().Get("bar_test")
	var count = make([]interface{}, 0)
	_, err := manager.ReadSingle(&count, "SELECT COUNT(1) FROM users", nil, nil)
	assert.Nil(t, err)
	assert.EqualValues(t, 1, count[0], "Shoud have one user after datastore reset")
}

func TestPopulateDatastore(t *testing.T) {
	datasetTestManager := Init(t)
	datasetFactory := datasetTestManager.DatasetFactory()
	{
		dataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			},
			map[string]interface{}{
				"id":       2,
				"username": "Bogi",
				"active":   false,
			},
			map[string]interface{}{
				"id":       3,
				"username": "Logi",
				"active":   true,
			},
		)

		inserted, updated, deleted, err := datasetTestManager.PrepareDatastore(&dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				dataset,
			},
		})
		if err != nil {
			t.Fatalf("failed to populate db: %v\n", err)
		}
		assert.Equal(t, 2, inserted, "Should have 2 rows added")
		assert.Equal(t, 1, updated, "Should have 1 row updated")
		assert.Equal(t, 0, deleted, "Should have no deletes")

	}
	{ //unknown column errror

		dataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":         1,
				"username34": "Dudi",
				"active":     true,
				"comments":   "abc",
			})

		_, _, _, err := datasetTestManager.PrepareDatastore(&dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				dataset,
			},
		})
		assert.NotNil(t, err)
	}

}

func TestExpectsDatastoreBaisc(t *testing.T) {
	datasetTestManager := Init(t)
	datasetFactory := datasetTestManager.DatasetFactory()

	{ //Check first that pre populated user is as expected
		dataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Edi",
				"active":   true,
				"salary":   43000.00,
				"comments": "no comments",
			})

		violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				dataset,
			},
		})
		if err != nil {
			t.Fatalf("failed to test due to error:\n\t%v", err)
		}

		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations))

	}

	{ //updated the first user and add two more user, check all expected user as so.

		dataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			},
			map[string]interface{}{
				"id":       2,
				"username": "Bogi",
				"active":   false,
			},
			map[string]interface{}{
				"id":       3,
				"username": "Logi",
				"active":   true,
			},
		)

		_, _, _, err := datasetTestManager.PrepareDatastore(&dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				dataset,
			},
		})
		assert.Nil(t, err)

		violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				dataset,
			},
		})
		if err != nil {
			t.Fatalf("failed to test due to error:\n\t%v", err)
		}
		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations.String()))
	}
}

func TestExpectsDatastoreWithAutoincrementMacro(t *testing.T) {
	datasetTestManager := Init(t)
	datasetFactory := datasetTestManager.DatasetFactory()

	{
		//add three more user, check all expected user as so.

		initDataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			},
			map[string]interface{}{
				"username": "Bogi",
				"active":   false,
			},
			map[string]interface{}{
				"username": "Logi",
				"salary":   11302,
				"active":   true,
				"comments": "<ds:sql [\"SELECT CURRENT_DATE\"]>",
			},
		)

		inserted, updated, _, err := datasetTestManager.PrepareDatastore(&dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				initDataset,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 3, inserted)
		assert.Equal(t, 0, updated)

		expectedDataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Edi",
				"active":   true,
				"comments": "no comments",
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Bogi",
				"active":   false,
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Logi",
				"active":   true,
				"salary":   "<ds:between [11301, 11303]>",
				"comments": "<ds:sql [\"SELECT CURRENT_DATE\"]>",
			},
		)
		violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				expectedDataset,
			},
		})
		if err != nil {
			t.Fatalf("failed to test sequence macro due to error:\n\t%v", err)
		}

		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations.String()))

	}

	{

		predicate := toolbox.NewBetweenPredicate(11301, 11303)
		expectedDataset := datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Edi",
				"active":   true,
				"comments": "no comments",
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Bogi",
				"active":   false,
			},
			map[string]interface{}{
				"id":       "<ds:seq [\"users\"]>",
				"username": "Logi",
				"salary":   &predicate,
				"active":   true,
				"comments": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
			},
		)

		violations, err := datasetTestManager.ExpectDatasets(dsunit.SnapshotDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []*dsunit.Dataset{
				expectedDataset,
			},
		})
		if err != nil {
			t.Fatalf("failed to test sequence macro due to error:\n\t%v", err)
		}
		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations))
	}

	//{
	//	initDataset := datasetFactory.CreateFromMap(gbqDatasetId, "log_history",
	//		map[string]interface{} {
	//			"event_id": 60,
	//			"event_name":   "measurable",
	//			"logging_create_time": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
	//		},
	//		map[string]interface{} {
	//			"event_id": 61,
	//			"event_name":   "view_in_1_second",
	//			"logging_create_time": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
	//		},
	//		map[string]interface{} {
	//			"event_id": 62,
	//			"event_name":   "view_in_5_seconds",
	//			"logging_create_time": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
	//		},
	//		map[string]interface{} {
	//			"event_id": 63,
	//			"event_name":   "view_in_10_seconds",
	//			"logging_create_time": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
	//		},
	//		map[string]interface{} {
	//			"event_id": 64,
	//			"event_name":   "view_in_15_seconds",
	//			"logging_create_time": "<ds:sql [\"SELECT CURRENT_TIMESTAMP\"]>",
	//		},
	//
	//	)
	//
	//	inserted, updated, _, err := datasetTestManager.PrepareDatastore(&dsunit.Datasets{
	//		Datastore: gbqDatasetId,
	//		Datasets: []*dsunit.Table{
	//			initDataset,
	//		},
	//	})
	//	assert.Nil(t, err)
	//	assert.Equal(t, 5, inserted)
	//	assert.Equal(t, 0, updated)
	//
	//
	//	expectedDataset := datasetFactory.CreateFromMap(gbqDatasetId, "log_history",
	//		map[string]interface{}{
	//			"position":"<ds:pos [\"log_history\"]>",
	//			"event_id":60,
	//			"event_name":"measurable",
	//		},
	//		map[string]interface{}{
	//			"position":"<ds:pos [\"log_history\"]>",
	//			"event_id":61,
	//			"event_name":"view_in_1_second",
	//		},
	//		map[string]interface{}{
	//			"position":"<ds:pos [\"log_history\"]>",
	//			"event_id":62,
	//			"event_name":"view_in_5_seconds",
	//		},
	//		map[string]interface{}{
	//			"position":"<ds:pos [\"log_history\"]>",
	//			"event_id":63,
	//			"event_name":"view_in_10_seconds",
	//		},
	//		map[string]interface{}{
	//			"position":"<ds:pos [\"log_history\"]>",
	//			"event_id":64,
	//			"event_name":"view_in_15_seconds",
	//		},
	//	)
	//
	//	violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
	//		Datastore: gbqDatasetId,
	//		Datasets: []*dsunit.Table{
	//			expectedDataset,
	//		},
	//	})
	//	if err != nil {
	//		t.Fatalf("failed to test sequence macro due to error:\n\t%v", err)
	//	}
	//
	//	assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations.String()))
	//
	//}

}

func TestRegisteredMapping(t *testing.T) {
	datasetTestManager := Init(t)
	mappings := datasetTestManager.RegisteredMapping()
	assert.Equal(t, []string{}, mappings)
}

func TestValueProviderRegistry(t *testing.T) {
	datasetTestManager := Init(t)
	valueProvider := datasetTestManager.ValueProviderRegistry()
	assert.True(t,  len(valueProvider.Names()) > 0)
}

func TestExecuteFromURL(t *testing.T) {
	datasetTestManager := Init(t)
	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/database.sql")
		_, err := datasetTestManager.ExecuteFromURL("bar_test", url)
		assert.Nil(t, err)
	}
	{
		url := dsunit.ExpandTestProtocolAsURLIfNeeded("test://test/non-existing.sql")
		_, err := datasetTestManager.ExecuteFromURL("bar_test", url)
		assert.NotNil(t, err)
	}
}
