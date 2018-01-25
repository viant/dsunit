package dsunit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"time"
)

func TestAssertDataset(t *testing.T) {

	tester := dsunit.DatasetTester{}

	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	actual := datasetFactory.Create(descriptor,
		map[string]interface{}{
			"id":       1,
			"username": "Dudi",
			"active":   true,
			"comments": "abc",
		},
		map[string]interface{}{
			"id":       3,
			"username": "Togi",
			"active":   true,
			"comments": "abc",
		},
	)

	expected := datasetFactory.Create(descriptor,
		map[string]interface{}{
			"id":       1,
			"username": "Dudi",
			"active":   true,
			"comments": "abc",
		},
		map[string]interface{}{
			"id":       2,
			"username": "Bogi",
			"active":   false,
		},
		map[string]interface{}{
			"id":       3,
			"username": "Lori",
			"active":   false,
		},
	)
	violations := tester.Assert("bar", expected, actual)

	assert.Equal(t, 3, len(violations), "Should have 2 violations")

	{
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeInvalidRowCount, violation.Type)
		assert.Equal(t, "users", violation.Table)
		assert.Equal(t, "3", violation.Expected)
		assert.Equal(t, "2", violation.Actual)
		assert.Equal(t, "", violation.Key)
	}

	{
		violation := violations[1]
		assert.Equal(t, dsunit.ViolationTypeMissingActualRow, violation.Type)
		assert.Equal(t, "users", violation.Table)
		assert.Equal(t, "{active:false, id:2, username:\"Bogi\"}", violation.Expected)
		assert.Equal(t, "", violation.Actual)
		assert.Equal(t, "2", violation.Key)

	}

	{
		violation := violations[2]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)
		assert.Equal(t, "users", violation.Table)
		assert.Equal(t, "{active:false,username:\"Lori\"}", violation.Expected)
		assert.Equal(t, "{active:true,username:\"Togi\"}", violation.Actual)
		assert.Equal(t, "3", violation.Key)

	}

}

func TestAssertMap(t *testing.T) {
	tester := dsunit.DatasetTester{}

	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string]int{
					"a": 1,
					"b": 2,
				},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string]int{
					"a": 10,
					"b": 2,
				},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string][]int{
					"a": {1, 3},
					"b": {1, 3},
				},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string][]int{
					"a": {1, 30},
					"b": {1, 3},
				},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}
	{ // equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string]int{
					"a": 1,
					"b": 2,
				},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string]int{
					"a": 1,
					"b": 2,
				},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}
	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string][]int{
					"a": {1, 3},
					"b": {1, 3},
				},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"settings": map[string][]int{
					"a": {1, 3},
					"b": {1, 3},
				},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

}

func TestAssertPredicate(t *testing.T) {
	tester := dsunit.DatasetTester{}
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}
	now := time.Now()
	predicate := dsunit.NewWithinPredicate(now, 10, "")
	var predicate2 toolbox.Predicate = dsunit.NewWithinPredicate(now, 10, "")
	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now.Add(time.Minute),
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": &predicate,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": &predicate2,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}
}

func TestAssertBool(t *testing.T) {
	tester := dsunit.DatasetTester{}
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": true,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "false",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": false,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1.2",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": true,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": false,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		// equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "true",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": true,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))
	}

	{
		// equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": true,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))
	}

}

func TestAssertPointer(t *testing.T) {
	tester := dsunit.DatasetTester{}
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{
		//not equal
		expectedValue := "2"
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": &expectedValue,
			},
		)

		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		//not equal
		actualValue := "2"
		expectedValue := 1
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": &actualValue,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": &expectedValue,
			},
		)

		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}
}

func TestAssertInt(t *testing.T) {
	tester := dsunit.DatasetTester{}
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "2",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}
	{
		//not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 2.3,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{
		// equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

	{
		// equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "false",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 0,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}
	// equal
	{
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1.0",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

}

func TestAssertNil(t *testing.T) {
	tester := dsunit.DatasetTester{}
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": nil,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "a",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}
	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "a",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":  1,
				"ts":  nil,
				"abc": 1,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": nil,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": nil,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))
	}

}

func TestAssertFloat(t *testing.T) {
	tester := dsunit.DatasetTester{}

	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1.2,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1.4,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1.2,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1.4",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1.2,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "true",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ // equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": 1.2,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": "1.2",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))
	}
}

func TestAssertTime(t *testing.T) {
	tester := dsunit.DatasetTester{}
	now := time.Now()
	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now.Unix() + 1,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //not equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": true,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}

	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now.String(),
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": toolbox.AsString(now.Unix()),
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

	{ //equal
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": toolbox.AsString(now.Unix()),
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id": 1,
				"ts": now,
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))

	}

}

func TestAssertSlice(t *testing.T) {
	tester := dsunit.DatasetTester{}

	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //not equal case
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":    1,
				"slice": []int{1, 2, 3},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":    1,
				"slice": []int{2, 3},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)
	}
	{ // equal case
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":    1,
				"slice": []int{1, 2, 3},
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":    1,
				"slice": []int{1, 2, 3},
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 0, len(violations))
	}

}

func TestAssertRegexp(t *testing.T) {
	tester := dsunit.DatasetTester{}

	var datasetFactory dsunit.DatasetFactory = dsunit.NewDatasetTestManager().DatasetFactory()
	descriptor := &dsc.TableDescriptor{Table: "users", Autoincrement: true, PkColumns: []string{"id"}}

	{ //string matches the pattern
		actual := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":       1,
				"username": "01Judi00",
			},
		)

		expected := datasetFactory.Create(descriptor,
			map[string]interface{}{
				"id":       1,
				"username": "01Regexp[A-Za-z]+00",
			},
		)
		violations := tester.Assert("bar", expected, actual)
		assert.Equal(t, 1, len(violations))
		violation := violations[0]
		assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)

	}
	//
	//{ //not a match
	//	actual := datasetFactory.Create(descriptor,
	//		map[string]interface{}{
	//			"id": 1,
	//			"username": "Ab123ui",
	//		},
	//	)
	//
	//	expected := datasetFactory.Create(descriptor,
	//		map[string]interface{}{
	//			"id": 1,
	//			"username": "Regexp[0-9]+",
	//		},
	//	)
	//	violations := tester.Assert("bar", expected, actual)
	//	assert.Equal(t, 1, len(violations))
	//	violation := violations[0]
	//	assert.Equal(t, dsunit.ViolationTypeRowNotEqual, violation.Type)
	//
	//}

}

func TestAssertViolations(t *testing.T) {

	var violationsSlice = make([]*dsunit.AssertViolation, 0)
	voilations := dsunit.NewAssertViolations(violationsSlice)
	assert.False(t, voilations.HasViolations())
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeInvalidRowCount, Expected: "3", Actual: "2", Key: "1"})
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeMissingActualRow, Expected: "a", Actual: "", Key: "1"})
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeRowNotEqual, Expected: "a", Actual: "b", Key: "1"})
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeInvalidRowCount, Expected: "3", Actual: "2", Key: "2"})
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeMissingActualRow, Expected: "a", Actual: "", Key: "2"})
	violationsSlice = append(violationsSlice, &dsunit.AssertViolation{Type: dsunit.ViolationTypeRowNotEqual, Expected: "a", Actual: "b", Key: "2"})
	voilations = dsunit.NewAssertViolations(violationsSlice)
	assert.True(t, voilations.HasViolations())
	assert.Equal(t, 6, len(voilations.Violations()))
	assert.True(t, len(voilations.String()) > 0)
}
