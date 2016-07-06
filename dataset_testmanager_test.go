package dsunit_test

import (
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
)

func Init(t *testing.T) dsunit.DatasetTestManager {
	datasetTestManager := dsunit.NewDatasetTestManager()
	managerRegistry := datasetTestManager.ManagerRegistry()
	managerFactory := dsc.NewManagerFactory()

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
	err := datasetTestManager.ClearDatastore("mysql", "bar_test")
	if err != nil {
		t.Fatalf("Failed to RecreateDatastore %v", err)
	}

	_, err = datasetTestManager.Execute(&dsunit.Script{
		Datastore: "bar_test",
		SQLs: []string{
			"DROP TABLE IF EXISTS users",
			"CREATE TABLE `users` (`id` INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,`username` varchar(255) DEFAULT NULL,`active` tinyint(1) DEFAULT '1',`salary` decimal(7,2) DEFAULT NULL,`comments` text,`last_access_time` timestamp DEFAULT CURRENT_TIMESTAMP)",
			"INSERT INTO users(username, active, salary, comments, last_access_time) VALUES('Edi', 1, 43000, 'no comments',CURRENT_TIMESTAMP);",
		},
	})

	if err != nil {
		t.Fatalf("Failed to init databsae %v", err)
	}
	datasetTestManager.RegisterTable("bar_test", &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}})
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
	dataset := *datasetFactory.CreateFromMap("bar_test", "users",
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
		Datasets: []dsunit.Dataset{
			dataset,
		},
	})
	if err != nil {
		t.Fatalf("Failed to populate db: %v\n", err)
	}
	assert.Equal(t, 2, inserted, "Should have 2 rows added")
	assert.Equal(t, 1, updated, "Should have 1 row updated")
	assert.Equal(t, 0, deleted, "Should have no deletes")

}

func TestExpectsDatastoreBaisc(t *testing.T) {
	datasetTestManager := Init(t)
	datasetFactory := datasetTestManager.DatasetFactory()

	{ //Check first that pre populated user is as expected
		dataset := *datasetFactory.CreateFromMap("bar_test", "users",
			map[string]interface{}{
				"id":       1,
				"username": "Edi",
				"active":   true,
				"salary":   43000.00,
				"comments": "no comments",
			})

		violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []dsunit.Dataset{
				dataset,
			},
		})
		if err != nil {
			t.Fatalf("Failed to test due to error:\n\t%v", err)
		}

		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations))

	}

	{ //updated the first user and add two more user, check all expected user as so.

		dataset := *datasetFactory.CreateFromMap("bar_test", "users",
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
			Datasets: []dsunit.Dataset{
				dataset,
			},
		})
		assert.Nil(t, err)

		violations, err := datasetTestManager.ExpectDatasets(dsunit.FullTableDatasetCheckPolicy, &dsunit.Datasets{
			Datastore: "bar_test",
			Datasets: []dsunit.Dataset{
				dataset,
			},
		})
		if err != nil {
			t.Fatalf("Failed to test due to error:\n\t%v", err)
		}
		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations.String()))
	}
}

func TestExpectsDatastoreWithAutoincrementMacro(t *testing.T) {
	datasetTestManager := Init(t)
	datasetFactory := datasetTestManager.DatasetFactory()

	{
		//add three more user, check all expected user as so.

		initDataset := *datasetFactory.CreateFromMap("bar_test", "users",
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
			Datasets: []dsunit.Dataset{
				initDataset,
			},
		})
		assert.Nil(t, err)
		assert.Equal(t, 3, inserted)
		assert.Equal(t, 0, updated)

		expectedDataset := *datasetFactory.CreateFromMap("bar_test", "users",
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
			Datasets: []dsunit.Dataset{
				expectedDataset,
			},
		})
		if err != nil {
			t.Fatalf("Failed to test sequence macro due to error:\n\t%v", err)
		}

		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations.String()))

	}

	{

		predicate := dsc.NewBetweenPredicate(11301, 11303)
		expectedDataset := *datasetFactory.CreateFromMap("bar_test", "users",
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
			Datasets: []dsunit.Dataset{
				expectedDataset,
			},
		})
		if err != nil {
			t.Fatalf("Failed to test sequence macro due to error:\n\t%v", err)
		}
		assert.False(t, violations.HasViolations(), fmt.Sprintf("V:%v\n", violations))
	}

}
