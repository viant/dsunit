package dsunit_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/dsunit"
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
