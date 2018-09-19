package with_dll

import (
	_ "github.com/viant/bgc"
	"github.com/viant/dsunit"
	"testing"
)

/*
Prerequisites:

	BigQuery secret credentials
*/

func TestDsunit_BigQuery(t *testing.T) {

	if dsunit.InitFromURL(t, "config/init.yaml") {
		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		//some business logic

		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}
