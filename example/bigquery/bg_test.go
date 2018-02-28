package bigquery


import (
	"testing"
	_ "github.com/viant/bgc"
	"github.com/viant/dsunit"
)


/*
Prerequisites:

	BigQuery secret credentials
 */



func TestDsunit_BigQuery(t *testing.T) {

	if dsunit.InitFromURL(t, "config/init.json") {
		if ! dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		//some business logic

		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}
