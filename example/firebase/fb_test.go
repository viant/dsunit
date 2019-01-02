package firebase

import (
	_ "github.com/adrianwit/fbc"

	"github.com/viant/dsunit"
	"testing"
)

/*
Prerequisites:

	Firebase secret credentials
*/

func TestDsunit_FirebaseQuery(t *testing.T) {

	if dsunit.InitFromURL(t, "config/init.yaml") {
		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		//some business logic

		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}
