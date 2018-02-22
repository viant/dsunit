// Package dsunit - Datastore testing library.
package dsunit

/*

DsUnit provides ability to build integration tests with the final datastore used by your application/ETL process.
No mocking, but actual test against various datastores likes sql RDBMS, Aerospike, BigQuery and structured transaction/log files.



Usage:

    dsunit.InitFromURL(t, "test/datastore_init.json")
    dsunit.PrepareFromURL(t, "test/use_case1/data.json")


    ... business test logic comes here

    dsunit.ExpectFromURL(t, "test/use_case1/data.json")


*/
