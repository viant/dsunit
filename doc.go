// Package dsunit - Datastore testing library.
package dsunit

/*

DsUnit provides ability to build integration tests with the final datastore used by your application/ETL process.
No mocking, but actual test against various datastores likes sql RDBMS, Aerospike, BigQuery and structured transaction/log files.






Usage:

    dsunit.InitDatastoresFromUrl(t, "test://test/datastore_init.json")
    dsunit.ExecuteScriptFromUrl(t, "svn://test/vertica_script_request.json")
    dsunit.PrepareDatastore(t, "mytestdb")


    ... business test logic comes here

    dsunit.ExpectDatasets(t, "mytestdb", dsunit.SnapshotDatasetCheckPolicy)


*/
