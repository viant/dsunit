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

## Dataset transformation

Dataset transformation allows routing from one dataset, to many datasets using DatasetMapping
DatasetMapping is defined as part of DatastoreConfig and mapping is register at the initialization stage.

```go


type DatasetColumn struct {
	Name         string
	DefaultValue string
	FromColumn   string
	Required     bool
}


type DatasetMapping struct {
	Table        string
	Columns      []DatasetColumn
	Associations []DatasetMapping
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
