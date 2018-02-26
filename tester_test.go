package dsunit_test

import (
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"testing"
)

func TestTester_FromURL(t *testing.T) {
	var tester = dsunit.NewTester()

	toolbox.RemoveFileIfExist("test/tester/tester.db")
	tester.RegisterFromURL(t, "test/tester/register.json")
	tester.RecreateFromURL(t, "test/tester/recreate.json")
	tester.RunScriptFromURL(t, "test/tester/run_script.json")
	tester.RunSQLFromURL(t, "test/tester/run_sqls.json")
	tester.AddTableMappingFromURL(t, "test/tester/add_mapping.json")
	tester.InitFromURL(t, "test/tester/init.json")
	tester.PrepareFromURL(t, "test/tester/prepare.json")
	tester.ExpectFromURL(t, "test/tester/expect.json")
	tester.PrepareFor(t, "tester", "test/tester/data", "use_case_1")
	tester.ExpectFor(t, "tester", dsunit.FullTableDatasetCheckPolicy, "test/tester/data", "use_case_1")

}

func Test_Discovery(t *testing.T) {
	var tester = dsunit.NewTester()
	tester.InitFromURL(t, "test/tester/init.json")
	tester.PrepareDatastore(t, "tester")
	tester.ExpectDatasets(t, "tester", dsunit.SnapshotDatasetCheckPolicy)
}
