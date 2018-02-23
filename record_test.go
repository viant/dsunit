package dsunit_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/viant/dsunit"
	"github.com/viant/toolbox"
	"testing"
)

func TestRecord_AsMap(t *testing.T) {
	var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
	assert.Equal(t, map[string]interface{}{
		"k1": 1,
		"k2": 3,
	}, record.AsMap())
}

func TestRecord_Columns(t *testing.T) {
	var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
	assert.Equal(t, []string{"k1", "k2"}, record.Columns())
}

func TestRecord_HasColumns(t *testing.T) {
	var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
	assert.True(t, record.HasColumn("k2"))
	assert.False(t, record.HasColumn("k20"))
}

func TestRecord_IsEmpty(t *testing.T) {
	{
		var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
		assert.False(t, record.IsEmpty())
	}
	{
		var record = dsunit.Record(toolbox.Pairs("@directive@", "abc"))
		assert.True(t, record.IsEmpty())
	}
}

func TestRecord_Value(t *testing.T) {
	var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
	assert.EqualValues(t, 1, record.Value("k1"))
	assert.EqualValues(t, "1", record.ValueAsString("k1"))
	record.SetValue("k3", 30)
	assert.EqualValues(t, 30, record.Value("k3"))
}

func TestRecord_String(t *testing.T) {
	var record = dsunit.Record(toolbox.Pairs("k1", 1, "k2", 3, "@directive@", "abc"))
	assert.True(t, record.String() != "", record.String())
}
