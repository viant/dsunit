package dynamodb

import (
	_ "github.com/adrianwit/dyndb"
	"github.com/viant/dsunit"
	"testing"
)

/*
Prerequisites:

	AWS secret credentials
*/

func Test_Dsunit(t *testing.T) {

	if dsunit.InitFromURL(t, "config/init.yaml") {
		if !dsunit.PrepareFor(t, "mydb", "data", "use_case_1") {
			return
		}
		//some business logic

		dsunit.ExpectFor(t, "mydb", dsunit.FullTableDatasetCheckPolicy, "data", "use_case_1")
	}
}
