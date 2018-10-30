# Datastore Testibility (dsunit)

[![Datastore testibility library for Go.](https://goreportcard.com/badge/github.com/viant/dsunit)](https://goreportcard.com/report/github.com/viant/dsunit)
[![GoDoc](https://godoc.org/github.com/viant/dsunit?status.svg)](https://godoc.org/github.com/viant/dsunit)

This library is compatible with Go 1.5+

Please refer to [`CHANGELOG.md`](CHANGELOG.md) if you encounter breaking changes.

- [Introduction](#Introduction)
- [Motivation](#Motivation)
- [API Documentaion](#API-Documentation)
- [Examples](#examples)
- [License](#License)
- [Credits and Acknowledgements](#Credits-and-Acknowledgements)



<a name="Introduction"></a>
## Introduction

Data focused testing belongs to blackbox group, where the main interest goes down to the initial and final state of the datastore.

To set the initial state of ta datastore, this framework provies utilities to either create empty datastore, or to prepare it with 
dataset data to test that application works correctly.

The final state testing focuses on checking that a dataset data matches an expected set of values after application logic run.
In this case this library has ability to verify  either complete or snapshot state of a datastore.
While the first approach will be comparing all tables data with expected set of values, the latter will reduced verification to the range provided by expected dataset.


<a name="Motivation"></a>

## Motivation

This library has been design to provide easy and unified way of testing any datastore (SQL, NoSSQL,file logs) on any platform, language and on the cloud.
It simplifies test organization by dataset auto discovery used for datastore preparation and verification. 
Dataset data can be loaded from various sources like:  memory, local or remote csv, json files.
All dataset support macro expression to dynamically evaluate value of data i.e <ds:sql ["SELECT CURRENT_DATE()"]> 
On top of that expected data, can also use predicate expressions to delegate verification of the data values i.e. <ds:between [11301, 11303]>. 
Finally a dataset like a view can be used to store data for many datastore sources in in just one dataset file.

Datastore initialization and dataset data verification can by managed locally or remotely on remote data store unit test server.


<a name="Usage"></a>

##Usage


**Data setup and verification**

```go


import (
	"testing"
	"github.com/viant/dsunit"
	_ "github.com/go-sql-driver/mysql"
)


func TestSetup(t *testing.T) {


    dsunit.InitFromURL(t, "test/init.json")
	dsunit.RunScript(t, verticaRequest)
	dsunit.PrepareFromURL(t, "mytestdb")
	
	
	... business test logic comes here
	
	dsunit.ExpectFromURL(t, "mytestdb", dsunit.SnapshotDatasetCheckPolicy)
	
	
	
}

```


**Reverse engineer data setup and verification** 

```go

	registerResponse := service.Register(dsunit.NewRegisterRequest("db1",
			&dsc.Config{
				DriverName: "sqlite3",
				Descriptor: "[url]",
				Parameters: map[string]interface{}{
					"url": filename,
				},
			}))
	if registerResponse.Stats != "ok" {
		log.Fatal(registerResponse.Error)
	}
    
	response := service.Freeze(&dsunit.FreezeRequest{
			Datastore:"db1",
			DestURL:"/tmp/dn1/expect/users.json",
			SQL:"SELECT * FROM users",
    })
	
```  


**Tester methods**

| Service  Methods | Description | Request | Response |
| --- | --- | --- | --- |
| Register(t *testing.T, request *RegisterRequest) bool | register database connection |  [RegisterRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L46) | [RegisterResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L70)  |
| RegisterFromURL(t *testing.T, URL string) bool | as above, where JSON request is fetched from URL/relative path |  [RegisterRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L46) | [RegisterResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L70)  |
| Recreate(t *testing.T, request *RecreateRequest) bool | recreate database/datastore |  [RecreateRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L76) | [RecreateResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L98)  |    
| RecreateFromURL(t *testing.T, URL string) bool | as above, where JSON request is fetched from URL/relative path |  [RecreateRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L76) | [RecreateResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L98)  |
| RunSQL(t *testing.T, request *RunSQLRequest) bool | run SQL commands |  [RunSQLRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L103) | [RunSQLResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L126)  |
| RunSQLFromURL(t *testing.T, URL string) bool | as above, where JSON request is fetched from URL/relative path  |  [RunSQLRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L103) | [RunSQLResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L126)  |
| RunScript(t *testing.T, request *RunScriptRequest) bool | run SQL script |  [RunScriptRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L132) | [RunSQLResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L126)  |
| RunScriptFromURL(t *testing.T, URL string) bool | as above, where JSON request is fetched from URL/relative path |  [RunScriptRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L132) | [RunSQLResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L126)  |
| AddTableMapping(t *testing.T, request *MappingRequest) bool | register database table mapping (view), |  [MappingRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L155) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L217)  |
| AddTableMappingFromURL(t *testing.T, URL string) bool | as above, where  JSON request is fetched from URL/relative path |  [MappingRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L155) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L217)  |
| Init(t *testing.T, request *InitRequest) bool | initialize datastore (register, recreate, run sql, add mapping) |  [InitRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L225) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L286)  |
| InitFromURL(t *testing.T, URL string) bool | as above, where  JSON request is fetched from URL/relative path |  [InitRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L225) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L286)  |
| Prepare(t *testing.T, request *PrepareRequest) bool | populate databstore with provided data |  [PrepareRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L293) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L323)  |
| PrepareFromURL(t *testing.T, URL string) bool | as above, where  JSON request is fetched from URL/relative path  |  [PrepareRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L293) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L323)  |
| PrepareDatastore(t *testing.T, datastore string) bool | match to populate all data files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name |  n/a | n/a  |
| PrepareFor(t *testing.T, datastore string, baseDirectory string, method string) bool |  match to populate all data files that are located in baseDirectory with method name |  n/a | n/a  |
| Expect(t *testing.T, request *ExpectRequest) bool | verify databstore with provided data |  [ExpectRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L340) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L380)  |
| ExpectFromURL(t *testing.T, URL string) bool | as above, where JSON request is fetched from URL/relative path |  [ExpectRequest](https://github.com/viant/dsunit/blob/master/service_contract.go#L340) | [MappingResponse](https://github.com/viant/dsunit/blob/master/service_contract.go#L380)  |
| ExpectDatasets(t *testing.T, datastore string, checkPolicy int) bool | match to verify all data files that are in the same location as a test file, with the same test file prefix, followed by lowe camel case test name |  n/a | n/a  |
| ExpectFor(t *testing.T, datastore string, checkPolicy int, baseDirectory string, method string) bool |   match to verify all dataset files that are located in the same directory as the test file with method name  |  n/a | n/a  |



## Validation

This library uses [assertly](https://github.com/viant/assertly) as the undelying validation mechanism 


### Macros

The macro is an expression with parameters that expands original text value. 
The general format of macro: &lt;ds:MACRO_NAME [json formated array of parameters]>

The following macro are build-in:


| Name | Parameters | Description | Example | 
| --- | --- | --- | --- |
| sql | SQL expression | Returns value of SQL expression | &lt;ds:sql["SELECT CURRENT_DATE()"]> |
| seq | name of sequence/table for autoicrement| Returns value of Sequence| &lt;ds:seq["users"]> |




### Predicates

Predicate allows expected value to be evaluated with actual dataset value using custom predicate logic.


| Name | Parameters | Description | Example | 
| --- | --- | --- | --- |
| between | from, to values | Evaluate actual value with between predicate | &lt;ds:between[1.888889, 1.88889]> |
| within_sec | base time, delta, optional date format | Evaluate if actual time is within delta of the base time | &lt;ds:within_sec["now", 6, "yyyyMMdd HH:mm:ss"]> |



### Directives


#### Data preparation

Most SQL drivers provide meta data about autoincrement, primary key, however if this is not available or partial verification with SQL is used, 
the following directive come handy. 

**@autoincrement@**

Allows specifying autoincrement field    



```json
[
  {"@autoincrement@":"id"},
  {"id":1, "username":"Dudi", "active":true, "salary":12400, "comments":"abc","last_access_time": "2016-03-01 03:10:00"},
  {"id":2, "username":"Rudi", "active":true, "salary":12600, "comments":"def","last_access_time": "2016-03-01 05:10:00"}
]

```

**@indexBy@**

(see also asserly indexBy directive usage, for nested data structe validation) 

Allows specifying pk fields


```json

[
  {"@indexBy@":["id"]},
  {"id":1, "username":"Dudi", "active":true, "salary":12400, "comments":"abc","last_access_time": "2016-03-01 03:10:00"},
  {"id":2, "username":"Rudi", "active":true, "salary":12600, "comments":"def","last_access_time": "2016-03-01 05:10:00"}
]

```

#### Data validation.


**@fromQuery@** 

Allows specified query to fetch actual dataset to be validated against expected dataset


**users.json**

```json
[
  {"@fromQuery@":"SELECT *  FROM users where id <= 2 ORDER BY id"},
  {"id":1, "username":"Dudi", "active":true, "salary":12400, "comments":"abc","last_access_time": "2016-03-01 03:10:00"},
  {"id":2, "username":"Rudi", "active":true, "salary":12600, "comments":"def","last_access_time": "2016-03-01 05:10:00"}
]
```




<a name="API-Documentation"></a>
## API Documentation

API documentation is available in the [`docs`](docs/README.md) directory.

## GoCover

[![GoCover](https://gocover.io/github.com/viant/dsunit)](https://gocover.io/github.com/viant/dsunit)


<a name="examples"></a>
## Examples
This project provide a various datasore **dsunit** integration examples (some with docker vi endly).

### RDBMS
* [Big Query](example/bigquery/)
* [MySQL](example/mysql)
* [Oracle](example/ora)
* [Postgres](example/pg)

## NoSQL
* [Aerospike](example/aerospike/)
* [MongoDB](example/mongo)


**External projects:**:
 
* [Simple CRUD app with dsunit](https://github.com/viant/dsc/tree/master/examples)


<a name="License"></a>
## License

The source code is made available under the terms of the Apache License, Version 2, as stated in the file `LICENSE`.

Individual files may be made available under their own specific license,
all compatible with Apache License, Version 2. Please see individual files for details.


<a name="Credits-and-Acknowledgements"></a>

##  Credits and Acknowledgements

**Library Author:** Adrian Witas

**Contributors:** Sudhakaran Dharmaraj
