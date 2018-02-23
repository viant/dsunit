package dsunit_test

import (
	"testing"
	"github.com/viant/dsunit"
	"github.com/viant/assertly"
	"github.com/stretchr/testify/assert"
	"github.com/viant/toolbox"
	"path"
	"strings"
)

func TestNewDataset(t *testing.T) {

	{
		dataset := dsunit.NewDataset("table1",
			map[string]interface{}{
				assertly.IndexByDirective:     []string{"id"},
				dsunit.AutoincrementDirective: "id",
			},
			map[string]interface{}{

			},
			map[string]interface{}{
				"id":       1,
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
				"@source@": "pk:1",
			},

			map[string]interface{}{
				"id":       2,
				"username": "Bogi",
				"active":   false,
				"email":    "a@as.ws",
			}, )

		assert.Equal(t, "table1", dataset.Table)
		assert.True(t, dataset.Records.Autoincrement())
		assert.True(t, dataset.Records.ShouldDeleteAll())
		assert.Equal(t, []string{"id"}, dataset.Records.UniqueKeys())
		assert.Equal(t, []string{"active", "comments", "email", "id", "username"}, dataset.Records.Columns())

		context := toolbox.NewContext()
		records, err := dataset.Records.Expand(context, false)
		if assert.Nil(t, err) {
			assert.Equal(t, 2, len(records))
			assert.EqualValues(t, map[string]interface{}{
				"id":       1,
				"username": "Dudi",
				"active":   true,
				"comments": "abc",
			}, records[0])
		}
	}
	{
		dataset := dsunit.NewDataset("table1",
			map[string]interface{}{
				assertly.IndexByDirective: "id",
				dsunit.FromQueryDirective: "SELECT * FROM table1",
			},
			map[string]interface{}{
				"id":       1,
				"username": "Dudi",
				"active":   true,
				"@source@": "pk:1",
			},
			map[string]interface{}{
				"id":       2,
				"username": "Bogi",
				"active":   false,
				"email":    "a@as.ws",
			}, )

		assert.Equal(t, "table1", dataset.Table)
		assert.False(t, dataset.Records.Autoincrement())
		assert.Equal(t, []string{"id"}, dataset.Records.UniqueKeys())
		assert.Equal(t, []string{"active", "email", "id", "username"}, dataset.Records.Columns())
		assert.EqualValues(t, "SELECT * FROM table1", dataset.Records.FromQuery())

	}
}

func TestNewDatasetResource_Load(t *testing.T) {
	baseDirectory := toolbox.CallerDirectory(3)
	datasetResource := dsunit.NewDatasetResource("db1", path.Join(baseDirectory, "test", "load"), "prefix_", "")

	if assert.Nil(t, datasetResource.Load()) {
		assert.EqualValues(t, 4, len(datasetResource.Datasets))
		for _, dataset := range datasetResource.Datasets {
			context :=toolbox.NewContext()
			records, err := dataset.Records.Expand(context, false)
			assert.Nil(t, err)
			assert.EqualValues(t, 3, len(records))
			assert.True(t, strings.HasPrefix(dataset.Table, "user"), dataset.Table)
		}
	}

}
