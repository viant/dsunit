# Introduction

This package describes the datastore unit (dsunit) API in detail.




## Datastore initialization

Before preparing dataset, dsunit needs to know what datastore is being used including details about all tables used in tests.
The easiest way to provide that is to create a test directory with datastore_init.json. 
This JSON represent  [InitDatastoresRequest](./../api.go#InitDatastoresRequest).

Please refer to [datastore_init.json](./../example/datastore_init.json) as example for various datatsore.
Note that different datastore implementation needs different configuration parameters.
Note that datastore  clear operation is only possible on datastores, which name contain 'test' keyword. 
This is safety meassures to avoid droping schema on staging/production datastore.



```go


import (
	"testing"
	"github.com/viant/dsunit"
	_ "github.com/go-sql-driver/mysql"
)


func TestSetup(t *testing.T) {

    dsunit.InitDatastoresFromUrl(t, init.json)
    // ...
    
}
```



## Dataset auto discovery

Dataset is defined as follow:

```go
type Row struct {
	Values map[string]interface{}
	Source string
}

type Dataset struct {
	dsc.TableDescriptor
	Rows []Row
}
```

Each table uses its own dataset instance with rows.

Datasets are used to prepare data in datastore, and to verify expected state of datastore.

In the preparation stage all values from datasets will be persisted in datastore.
If dataset is listed with no values then underlying data will be removed from the datastore.




There are two way of organizing your test datasets, that will be auto discovered by this library.

The first one expect the dataset files be placed in the same directory as your test file, starting with exactly the same name (without .go extension), followed by lower case undescore name of the test method that will be using the data, and eiter  prepare or expect keyword ending with table name and format file.

For instance is a tester wrote a test method TestSubmit in service_test.go file, the auto-discovery matches any file with the following pattern:
    
    service_test_submit_[prepare|expect]_&lt;table_name>.[json|csv]
    


```go

func TestSubmit(t *.testing.T) {

    ...
	dsunit.PrepareDatastore(t, "mystore")
	
	// business logic comes here
	
	dsunit.ExpectDatasets(t, "mystore", dsunit.SnapshotDatasetCheckPolicy)

}
```


The second way of organizing test is you use custom directory, I would recommend a separate test directory.
In this case auto-discovery matches all dataset files that are located in test directory with the following pattern

  &lt;test_directory_path>&lt;lower_underscore method name>_[prepare|expect]_&lt;table>.[json|csv]
  
  For instance if test method is called TestPersistAll

  &lt;test_directory_path>persist_all_prepare_users.json
  &lt;test_directory_path>persist_all_prepare_permissions.json

  &lt;test_directory_path>persist_all_expect_users.json
  
  
```go

func TestPersistAll(t *.testing.T) {
    ...
    dsunit.PrepareDatastoreFor(t, "bar_test", "test/", "PersistAll")
  

  	//business logic comes here
  
  	dsunit.ExpectDatasetFor(t, "bar_test", dsunit.SnapshotDatasetCheckPolicy,"test/", "PersistAll")

}  
```

<a name="mapping">&nbsp;</a>
## Dataset transformation with multi table mapping

Imagine your application uses a dozen tables,  that needs to be seeded with data per each test use case.
In reality, most of these tables may have many default value, and if only handful column (except IDs) have specific values for test use cases. 
Table mapping allows you to use one virtual table with virtual columns that are mapped to actual table/column.

Imagine there is v_table mapping to 4 tables defined as follow:

```json
{
  "Name": "v_table",
  "Table": "table1",
  "Columns": [
    {
      "Name": "id",
      "FromColumn": "table1_id",
      "Required": true,
      "Unique": true
    },
    {
      "Name": "name"
    },
    {
      "Name": "enabled",
      "DefaultValue": "1"
    },
    {
      "Name": "cost",
      "DefaultValue": "<ds:nil>"
    }
  ],
  "Associations": [
    {
      "Table": "table2",
      "Columns": [
        {
          "Name": "id",
          "Required": true,
          "FromColumn": "table2_id",
          "Unique": true
        },
        {
          "Name": "name",
          "FromColumn": "table2_name"
        }
      ],
      "Associations": [
        {
          "Table": "table3",
          "Columns": [
            {
              "Name": "table2_id",
              "Required": true,
              "Unique": true
            },
            {
              "Name": "table3_id",
              "Required": true,
              "Unique": true
            }
          ]
        },
        {
          "Table": "table4",
          "Columns": [
            {
              "Name": "table2_id",
              "Required": true,
              "Unique": true
            },
            {
              "Name": "table4_id",
              "Required": true,
              "Unique": true
            }
          ]
        }
      ]
    }
  ]
}

``` 


The following virtual record would create entry in table1, table2 and table4. Note that table3 is skipped since v_table does not define required column table3_id.

@v_table.json
```json
  [
      {
          "Table": "v_table",
          "Value": [{
            "table1_id": "$seq.table1",
            "name": "Name 1",
            "table2_id": "$seq.table2",
            "table2_name": "other name 1",
            "table4_id": "$table4_Id"
          }],
          "PostIncrement": [
            "seq.table1",
            "seq.table2"
          ],
         "AutoGenerate": {
           "table4_Id": "uuid.next"
         },
        "Key": "${tagId}_table1"
    }
  ]
```

_Expanded mappings:_ 

```json

  {
    "table1": {
      "id": "$seq.table1",
      "name": "Name 1",
      "enabled": 1,
      "cost": null
    },
    "table2": {
      "id": "$seq.table2",
      "table1_id": "$seq.table1",
      "name": "other name 1"
    },
    "table4": {
      "id": "$table4_idId",
      "table2_id": "$seq.table2"
    }
  }


```
    
    



## Local and Remote Datastore unit mode

Start [DS Unit Server](./../main/dsunit-server.go).

To run dsunit in remote mode add the following code in you test file


```go

func init() {
	//dsunit.UseRemoteTestServer("http://localhost:&lt;port>")

}

```
