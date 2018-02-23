package dsunit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewDatafileInfo(t *testing.T) {

	{
		info := NewDatafileInfo("test_s.json", "p", "s")
		assert.Nil(t, info)
	}
	{
		info := NewDatafileInfo("p_test.jsons", "p", "s")
		assert.Nil(t, info)
	}
	{
		info := NewDatafileInfo("p_test_s.json", "p_", "_s")
		if assert.NotNil(t, info) {
			assert.Equal(t, "test", info.Name)
			assert.Equal(t, "json", info.Ext)
			assert.Equal(t, "p_", info.Prefix)
			assert.Equal(t, "_s", info.Postfix)
		}
	}

}
