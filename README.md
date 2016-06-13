# Datastore Testibility (dsunit)

[![Datastore testibility library for Go.](https://goreportcard.com/badge/github.com/viant/dsunit)](https://goreportcard.com/report/github.com/viant/dsunit)

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Introduction](#Introduction)
- [Motivation](#Motivation)

- [API Documentaion](#API-Documentation)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)



<a name="Introduction"></a>
## Introduction

Data focused testing belongs to blackbox group, where the main interest goes down to the initial and final state of the datastore.

To set the initial state of ta datastore, this framework provies utilities to either create empty datastore, or to prepare it with 
dataset data to test that application works correctly.

The final state testing focuses on checking that a dataset data match an expected set of values after tests run.
In this case this library has ability to verify  either complete or snapshot state of a datastore.
While the first approach will be comparing all tables data with expected set of values, the latter will reduced verification to the range provided by expected dataset.


<a name="Motivation"></a>

## Motivation

This library has been design to provide easy and unified way of testing any datastore (SQL, NoSSQL) on any platform, language and on the cloud.
It simplifies test organization by auto discovery of dataset used for datastore preparation and verification. 
Dataset data can be loaded from various sources like:  memory, application domain classes, local or remote csv, json files.
It can use macro expression to dynamically evaluate value of data i.e <ds:sql ["SELECT CURRENT_DATE()"]> 

Expected data, can also use predicate expressions to delegate verification of the data values i.e. <ds:between [11301, 11303]>. 
Finally a dataset like a view can store data for many datastore sources in one place.

Datastore initialization and dataset data verification can by managed locally or remotely on remote data store unit test server.


```go


import (
	"testing"
	"github.com/viant/dsunit"
	_ "github.com/go-sql-driver/mysql"
)


func TestSetup(t *testing.T) {

    dsunit.InitDatastoresFromUrl(t, "test://test/datastore_init.json")
	dsunit.ExecuteScriptFromUrl(t, "test://test/script_request.json")
	dsunit.PrepareDatastore(t, "mytestdb")
	
	
	... business test logic comes here
	
	dsunit.ExpectDatasets(t, "mytestdb", dsunit.SnapshotDatasetCheckPolicy)
}
```


## API Documentation

API documentation is available in the [`docs`](docs/README.md) directory.



<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:** Sudhakaran Dharmaraj
