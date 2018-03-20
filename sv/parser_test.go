package sv

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseColumnarData(t *testing.T) {
	data := `id,name,delays,active,income
1,222,3,true,1232.3
2,A\"bc,8,FALSE,3
3,"Xb,c",8,,0.33`

	var parser = NewSeparatedValueParser(",")
	records, err := parser.Parse([]byte(data))
	if assert.Nil(t, err) {
		assert.Equal(t, 3, len(records))
		assert.EqualValues(t, map[string]interface{}{
			"id":     1,
			"name":   "222",
			"delays": 3,
			"active": true,
			"income": 1232.3,
		}, records[0])
		assert.EqualValues(t, map[string]interface{}{
			"id":     2,
			"name":   `A\"bc`,
			"delays": 8,
			"active": false,
			"income": 3.0,
		}, records[1])
		assert.EqualValues(t, map[string]interface{}{
			"id":     3,
			"name":   `Xb,c`,
			"delays": 8,
			"active": false,
			"income": 0.33,
		}, records[2])
	}
}
